using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Threading.Channels;
using System.Threading;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.IO;
using Spectre.Console;
using BlushingPenguin.JsonPath;

record DbCopierOptions
(
    CosmosClient SourceClient,
    CosmosClient DestClient,
    string SourceDatabase,
    string DestinationDatabase
)
{
    /// <summary>
    /// Execute N container copies in parallel
    /// </summary>
    public int MaxContainerParallel { get; init; } = 10;

    /// <summary>
    /// Enqueue N containers
    /// </summary>
    public int MaxContainerBufferSize { get; init; } = 10;

    /// <summary>
    /// Execute N document copies in parallel per container copy operation
    /// </summary>
    public int MaxDocCopyParallel { get; init; } = 10;

    /// <summary>
    /// Enqueue N documents per container copy operation
    /// </summary>
    public int MaxDocCopyBufferSize { get; init; } = 100;

    /// <summary>
    /// Enqueue N table display render updates (affects rendering only)
    /// </summary>
    public int UIRenderQueueCapacity { get; init; } = 1000;
}

class DbCopier
{
    static readonly RecyclableMemoryStreamManager streamManager = new ();

    public abstract record CopyDiagnostic(string container);
    public record CopyDiagnosticMessage(string container, string message, bool warning = false) : CopyDiagnostic(container);
    public record CopyDiagnosticProgress(string container, int progress, int total) : CopyDiagnostic(container);
    public record CopyDiagnosticDone(string container) : CopyDiagnostic(container);
    public record CopyDiagnosticFailed(string container, Exception exception) : CopyDiagnostic(container);

    /// <summary>
    /// Copy with interactive grid
    /// </summary>
    internal static async Task<bool> CopyWithDetails(DbCopierOptions options, CancellationToken cancellationToken = default)
    {
        try
        {
            if (!System.Diagnostics.Debugger.IsAttached)
                AnsiConsole.Console.Clear(false);

            var rows = new SortedList<string, CopyDiagnostic>();
            RenderCopyGrid(rows);
            
            var channel = Channel.CreateBounded<CopyDiagnostic>(
                new BoundedChannelOptions(options.UIRenderQueueCapacity) 
                { 
                    FullMode = BoundedChannelFullMode.DropOldest 
                });
            
            // Rate limiting render loop
            using var cancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            
            _ = Task.Run(async () => 
            {
                SortedList<string, CopyDiagnostic> copyBuffer = new();

                while (!cancellationSource.IsCancellationRequested)
                {
                    lock (rows)
                    {
                        if (rows.Except(copyBuffer).Any())
                        {
                            RenderCopyGrid(rows);

                            copyBuffer = new SortedList<string, CopyDiagnostic>(rows);
                        }
                    }

                    await Task.Delay(500, cancellationSource.Token);
                }
            });

            // For every message in the channel, update the grid
            await foreach (var diag in CopyInternal(options, cancellationToken))
            {
                lock (rows)
                {
                    rows.Remove(diag.container);
                    rows.Add(diag.container, diag);
                }
            }

            cancellationSource.Cancel();
            
            RenderCopyGrid(rows);

            return true;
        }
        catch (Exception error)
        {
            if (!System.Diagnostics.Debugger.IsAttached)
                AnsiConsole.Console.Clear(false);

            AnsiConsole.WriteException(error);
            return false;
        }
        
        static void RenderCopyGrid(SortedList<string, CopyDiagnostic> rows)
        {
            var table = new Table()
                .Expand()
                .Border(TableBorder.Horizontal)
                .AddColumn("Container")
                .AddColumn("Progress (%)")
                ;

            var any = false;
            foreach (var (container, diag) in rows)
            {
                any = true;
                
                table.AddRow(container, diag switch 
                {
                    CopyDiagnosticMessage(_, var message, var warning) => warning ? $"[yellow]{warning}[/yellow/" : message,
                    CopyDiagnosticProgress(_, var p, var total) => $"{(float)p/total:p0} ({p}/{total})",
                    CopyDiagnosticDone => $"[green]Done[/]",
                    CopyDiagnosticFailed(_, var e) => e switch
                    {
                        TaskCanceledException => "[yellow]Cancelled[/]",
                        CosmosException       => "[red]Failed (CosmosException)[/]",
                        _                     => "[red]Failed[/]"
                    },
                    _ => ""
                });
            }

            if (!any)
                table.AddRow("Starting...");

            if (!System.Diagnostics.Debugger.IsAttached)
                AnsiConsole.Console.Clear(false);

            AnsiConsole.Render(
                new FigletText("AzTableCopy")
                    .LeftAligned()
                    .Color(Color.Blue));

            AnsiConsole.Render(table);
        }
    }

    /// <summary>
    /// Copy with minimal output diagnostics
    /// </summary>
    internal static async Task<bool> CopyMinimal(DbCopierOptions options, CancellationToken cancellationToken = default)
    {
        try
        {
            Console.WriteLine("AzTableCopy");
            Console.WriteLine("Starting...");

            Stopwatch sw = new();
            sw.Start();

            await foreach (var diag in CopyInternal(options, cancellationToken))
            {
                
            }

            sw.Stop();

            Console.WriteLine($"Complete. Total elapsed: {sw.Elapsed.TotalSeconds:#,0.###} seconds");

            return true;
        }
        catch (Exception error)
        {
            Console.WriteLine(error);
            return false;
        }
    }

    private static async IAsyncEnumerable<CopyDiagnostic> CopyInternal(
        DbCopierOptions options, 
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var (sourceClient, destClient, sourceDatabase, destinationDatabase) = options;

        // Create destination database
        var sourceDbClient = sourceClient.GetDatabase(sourceDatabase);
        var destDbClient   = destClient.GetDatabase(destinationDatabase);

        var channel = Channel.CreateBounded<CopyDiagnostic>(
            new BoundedChannelOptions(options.UIRenderQueueCapacity) 
            { 
                FullMode = BoundedChannelFullMode.DropOldest 
            });

        // Execute copy
        var copyTask = CopyDataPipeline(channel.Writer);

        // For every message in the channel, update the grid
        await foreach (var item in channel.Reader.ReadAllAsync())
            yield return item;

        await copyTask;

        async Task CopyDataPipeline(ChannelWriter<CopyDiagnostic> channel)
        {
            var sourceDb = sourceClient.GetDatabase(sourceDatabase);

            var buffer = new BufferBlock<ContainerProperties>(new () 
            { 
                BoundedCapacity   = options.MaxContainerBufferSize, 
                CancellationToken = cancellationToken 
            });

            var action = new ActionBlock<ContainerProperties>(CopyContainerFactory(channel), new () 
            { 
                MaxDegreeOfParallelism    = options.MaxContainerParallel,
                SingleProducerConstrained = true,
                CancellationToken         = cancellationToken,
            });

            using (buffer.LinkTo(action, new() { PropagateCompletion = true }))
            {
                try
                {
                    try
                    {
                        await destClient.CreateDatabaseAsync(destinationDatabase);
                    }
                    catch (CosmosException)
                    {
                        channel.TryWrite(new CopyDiagnosticMessage("", "Unable to create destination database - may already be copied", warning: true));
                        return;
                    }

                    // Gather list of containers to copy
                    using var feed = sourceDb.GetContainerQueryIterator<ContainerProperties>();

                    while (feed.HasMoreResults && !cancellationToken.IsCancellationRequested)
                    {
                        foreach (var c in await feed.ReadNextAsync())
                        {
                            channel.TryWrite(new CopyDiagnosticMessage(c.Id, "Queued..."));

                            while (!buffer.Post(c))
                                await Task.Yield();
                        }
                    }
                }
                finally
                {
                    buffer.Complete();

                    await action.Completion.ContinueWith(task =>
                        channel.Complete()
                    );
                }
            }
        }

        Func<ContainerProperties, Task> CopyContainerFactory(ChannelWriter<CopyDiagnostic> messageChannel) => async (ContainerProperties c) =>
        {
            messageChannel.TryWrite(new CopyDiagnosticMessage(c.Id, "Creating container..."));

            try
            {
                await destDbClient.CreateContainerIfNotExistsAsync(c);

                messageChannel.TryWrite(new CopyDiagnosticMessage(c.Id, "Copying data..."));

                var sourceContainer = sourceDbClient.GetContainer(c.Id);
                var destContainer   = destDbClient.GetContainer(c.Id);

                // Getting number of documents
                var total = await sourceContainer.GetItemLinqQueryable<object>(true).CountAsync();

                if (total > 0)
                {
                    var processed = 0;
                    var (firstPart, pkPath) = c.PartitionKeyPath[1..].Replace('/', '.').SplitTwo('.');

                    var buffer = new BufferBlock<Document>(new () 
                    { 
                        BoundedCapacity   = options.MaxDocCopyBufferSize, 
                        CancellationToken = cancellationToken 
                    });

                    // Consumer
                    var consumer = new ActionBlock<Document>(
                        async item =>
                        {
                            using var stream = streamManager.GetStream();

                            using (var sw = new Utf8JsonWriter(stream))
                                JsonSerializer.Serialize(sw, item);

                            stream.Seek(0, SeekOrigin.Begin);

                            var pk   = item.GetPartitionKey(firstPart, pkPath);
                            var resp = await destContainer.CreateItemStreamAsync(stream, pk);

                            resp.EnsureSuccessStatusCode();

                            var p = Interlocked.Increment(ref processed);

                            messageChannel.TryWrite(new CopyDiagnosticProgress(c.Id, p, total));
                        },
                        new () 
                        { 
                            MaxDegreeOfParallelism    = options.MaxDocCopyParallel,
                            SingleProducerConstrained = true,
                            CancellationToken         = cancellationToken
                        });

                    using (buffer.LinkTo(consumer, new() { PropagateCompletion = true }))
                    {
                        try
                        {
                            // Retrieve all documents
                            using var docFeed = sourceContainer.GetItemQueryStreamIterator();

                            // Producer
                            while (docFeed.HasMoreResults && !cancellationToken.IsCancellationRequested)
                            {
                                using var response = await docFeed.ReadNextAsync();
                                
                                var all = await JsonSerializer.DeserializeAsync<DocumentContainer>(response.Content);

                                // Post
                                for (var i = 0; i < all!.Documents.Count; ++i)
                                {
                                    while (!buffer.Post(all.Documents[i]))
                                        await Task.Yield();
                                }
                            }
                        }
                        finally
                        {
                            buffer.Complete();

                            await consumer.Completion;
                        }
                    }
                }

                messageChannel.TryWrite(new CopyDiagnosticDone(c.Id));
            }
            catch (Exception e)
            {
                messageChannel.TryWrite(new CopyDiagnosticFailed(c.Id, e));
            }
        };
    }

    class DocumentContainer
    {
        public List<Document> Documents { get; set; } = default!;
    }

    class Document
    {
        [JsonIgnore, JsonPropertyName("_rid")]
        public string? Rid { get; set; }
        
        [JsonIgnore, JsonPropertyName("_self")]
        public string? Self { get; set; }

        [JsonIgnore, JsonPropertyName("_etag")]
        public int? ETag { get; set; }
        
        [JsonIgnore, JsonPropertyName("_attachments")]
        public string? Attachments { get; set; }
        
        [JsonIgnore, JsonPropertyName("_ts")]
        public int? Ts { get; set; }

        [JsonExtensionData]
        public Dictionary<string, JsonElement> Properties { get; set; } = default!;

        public PartitionKey GetPartitionKey(string firstPart, string? pkPath)
        {
            if (Properties.TryGetValue(firstPart, out var jsonToken))
            {
                if (pkPath is not null)
                {
                    if (jsonToken.SelectToken(pkPath) is JsonElement subToken)
                        return GetPK(subToken);
                }
                else
                    return GetPK(jsonToken);
            }
    
            return PartitionKey.None;

            static PartitionKey GetPK(JsonElement jsonToken)
            {
                return jsonToken.ValueKind switch
                {
                    JsonValueKind.String => new(jsonToken.GetString()),
                    JsonValueKind.False  => new(false),
                    JsonValueKind.True   => new(true),
                    JsonValueKind.Number => new(jsonToken.GetDouble()),
                    _                    => PartitionKey.None
                };
            }
        }
    }
}

static class StringExtensions
{
    /// <summary>
    /// Split into two strings
    /// </summary>
    public static (string lhs, string? rhs) SplitTwo(this string input, char id)
    {
        var index = input.IndexOf(id, 1);

        return index == -1
            ? (input, null)
            : (input[..index], input[(index + 1)..]);
    }
}