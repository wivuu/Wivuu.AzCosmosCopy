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

public record DbCopierOptions
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
    public int MaxDocCopyParallel { get; init; } = 100;

    /// <summary>
    /// Enqueue N documents per container copy operation
    /// </summary>
    public int MaxDocCopyBufferSize { get; init; } = 100;

    /// <summary>
    /// Enqueue N table display render updates (affects rendering only)
    /// </summary>
    public int UIRenderQueueCapacity { get; init; } = 1000;
}

public class DbCopier
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
            AnsiConsole.Render(
                new FigletText("AzCosmosCopy")
                    .LeftAligned()
                    .Color(Color.Blue));

            await AnsiConsole
                .Progress()
                .Columns(new ProgressColumn[]
                {
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new RemainingTimeColumn(),
                })
                .StartAsync(async ctx =>
                {
                    var tasks = new SortedDictionary<string, ProgressTask>();

                    await foreach (var diag in CopyAsync(options, cancellationToken))
                    {
                        if (!tasks.TryGetValue(diag.container, out var task))
                            tasks.Add(diag.container, task = ctx.AddTask(diag.container, new () { MaxValue = 1 }));

                        switch (diag)
                        {
                            case CopyDiagnosticProgress(var container, int progress, int total):
                                var lastAmount = task.Value * total;
                                task.Increment((progress - lastAmount) / total);
                                break;

                            case CopyDiagnosticMessage(var container, var message, var warning):
                                AnsiConsole.MarkupLine(
                                    $"{container} - " + 
                                    (warning ? $"[yellow]{Markup.Escape(message)}[/]" : Markup.Escape(message))
                                );
                                break;

                            case CopyDiagnosticDone(var container):
                                task.Increment(1);
                                task.StopTask();

                                AnsiConsole.MarkupLine(
                                    $"{container} - " +
                                    $"[green]Done ({task.ElapsedTime?.TotalSeconds:#,0.###}s)[/]"
                                );
                                break;

                            case CopyDiagnosticFailed(var container, var error):
                                break;
                        }
                    }
                });

            return true;
        }
        catch (Exception error)
        {
            AnsiConsole.WriteException(error);
            return false;
        }
    }

    /// <summary>
    /// Copy with minimal output diagnostics
    /// </summary>
    internal static async Task<bool> CopyMinimal(DbCopierOptions options, CancellationToken cancellationToken = default)
    {
        try
        {
            Console.WriteLine("AzCosmosCopy");
            Console.WriteLine("Starting...");

            Stopwatch sw = new();
            sw.Start();

            await foreach (var diag in CopyAsync(options, cancellationToken))
            {
                switch (diag)
                {
                    case CopyDiagnosticMessage(var container, var message, _):
                        AnsiConsole.WriteLine($"{container}\t\t- {message}");
                        break;
                    case CopyDiagnosticDone(var container):
                        AnsiConsole.WriteLine($"{container}\t\t- Done");
                        break;
                    case CopyDiagnosticFailed(var container, var error):
                        AnsiConsole.Write($"{container} -");
                        AnsiConsole.WriteException(error);
                        break;
                }
            }

            sw.Stop();

            Console.WriteLine($"Finished after {sw.Elapsed.TotalSeconds:#,0.###} seconds");

            return true;
        }
        catch (Exception error)
        {
            AnsiConsole.WriteException(error);
            return false;
        }
    }

    public static async IAsyncEnumerable<CopyDiagnostic> CopyAsync(
        DbCopierOptions options, 
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var (sourceClient, destClient, sourceDatabase, destinationDatabase) = options;

        var channel = Channel.CreateBounded<CopyDiagnostic>(
            new BoundedChannelOptions(options.UIRenderQueueCapacity) 
            { 
                FullMode = BoundedChannelFullMode.DropOldest 
            });

        // Execute copy
        var copyTask = CopyDataPipeline(channel.Writer);

        // For every message in the channel, yield back
        await foreach (var item in channel.Reader.ReadAllAsync())
            yield return item;

        await copyTask;

        // Pipeline which copies all containers
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

        // Pipeline which copies a single container
        Func<ContainerProperties, Task> CopyContainerFactory(ChannelWriter<CopyDiagnostic> messageChannel) => async (ContainerProperties c) =>
        {
            try
            {
                var sourceContainer = sourceClient.GetContainer(sourceDatabase, c.Id);
                var destContainer   = destClient.GetContainer(destinationDatabase, c.Id);

                // Create dest container
                await destClient.GetDatabase(destinationDatabase).CreateContainerAsync(c);

                messageChannel.TryWrite(new CopyDiagnosticMessage(c.Id, "Copying data..."));

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

                            var progress = Interlocked.Increment(ref processed);

                            messageChannel.TryWrite(new CopyDiagnosticProgress(c.Id, progress, total));
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

    /// <summary>
    /// Document response container
    /// </summary>
    class DocumentContainer
    {
        public List<Document> Documents { get; set; } = default!;
    }

    /// <summary>
    /// Single document in response
    /// </summary>
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