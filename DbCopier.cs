using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Threading.Channels;
using System.Threading;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Spectre.Console;
using Microsoft.IO;
using BlushingPenguin.JsonPath;

class DbCopier
{
    const int MaxContainerParallel = 10;
    const int MaxContainerBufferSize = 10;
    const int MaxDocCopyParallel = 100;
    const int MaxDocCopyBufferSize = 100;
    const int UIRenderQueueCapacity = 1000;
    static readonly RecyclableMemoryStreamManager streamManager = new ();

    record CopyRow(string container, string progress);

    static void RenderCopyGrid(SortedList<string, CopyRow> rows)
    {
        var table = new Table()
            .Expand()
            .Border(TableBorder.Horizontal)
            .AddColumn("Container")
            .AddColumn("Progress (%)")
            ;

        var any = false;
        foreach (var (container, (_, progress)) in rows)
        {
            any = true;
            table.AddRow(container, progress);
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

    internal static async Task<bool> CopyAsync(CosmosClient sourceClient, CosmosClient destClient, string sourceDatabase, string destinationDatabase, CancellationToken cancellationToken = default)
    {
        // Create destination database
        var sourceDbClient = sourceClient.GetDatabase(sourceDatabase);
        var destDbClient   = destClient.GetDatabase(destinationDatabase);

        try
        {
            if (!System.Diagnostics.Debugger.IsAttached)
                AnsiConsole.Console.Clear(false);

            var rows = new SortedList<string, CopyRow>();
            RenderCopyGrid(rows);
            
            var channel = Channel.CreateBounded<CopyRow>(
                new BoundedChannelOptions(UIRenderQueueCapacity) 
                { 
                    FullMode = BoundedChannelFullMode.DropOldest 
                });

            // Execute copy
            var copyTask = CopyDataPipeline(channel.Writer);
            
            // Rate limiting render loop
            using var tcs = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            
            _ = Task.Run(async () => 
            {
                SortedList<string, CopyRow> copyBuffer = new();

                while (!tcs.IsCancellationRequested)
                {
                    lock (rows)
                    {
                        if (rows.Except(copyBuffer).Any())
                        {
                            RenderCopyGrid(rows);

                            copyBuffer = new SortedList<string, CopyRow>(rows);
                        }
                    }

                    await Task.Delay(500, tcs.Token);
                }
            });

            // For every message in the channel, update the grid
            await foreach (var item in channel.Reader.ReadAllAsync())
            {
                lock (rows)
                {
                    rows.Remove(item.container);
                    rows.Add(item.container, item);
                }
            }

            await copyTask;
            tcs.Cancel();
            
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

        async Task CopyDataPipeline(ChannelWriter<CopyRow> channel)
        {
            var sourceDb = sourceClient.GetDatabase(sourceDatabase);

            var buffer = new BufferBlock<ContainerProperties>(new () 
            { 
                BoundedCapacity   = MaxContainerBufferSize, 
                CancellationToken = cancellationToken 
            });

            var action = new ActionBlock<ContainerProperties>(CopyContainerFactory(channel), new () 
            { 
                MaxDegreeOfParallelism    = MaxContainerParallel,
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
                        channel.TryWrite(new ("", "[yellow]Unable to create destination database - may already be copied[/]"));
                        return;
                    }

                    // Gather list of containers to copy
                    using var feed = sourceDb.GetContainerQueryIterator<ContainerProperties>();

                    while (feed.HasMoreResults && !cancellationToken.IsCancellationRequested)
                    {
                        foreach (var c in await feed.ReadNextAsync())
                        {
                            channel.TryWrite(new (c.Id, "Queued..."));

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

        Func<ContainerProperties, Task> CopyContainerFactory(ChannelWriter<CopyRow> messageChannel) => async (ContainerProperties c) =>
        {
            messageChannel.TryWrite(new (c.Id, "Creating container..."));

            try
            {
                await destDbClient.CreateContainerIfNotExistsAsync(c);

                messageChannel.TryWrite(new (c.Id, "Copying data..."));

                var sourceContainer = sourceDbClient.GetContainer(c.Id);
                var destContainer   = destDbClient.GetContainer(c.Id);

                // Getting number of documents
                var total = await sourceContainer.GetItemLinqQueryable<object>(true).CountAsync();

                if (total > 0)
                {
                    var processed = 0;
                    var (firstPart, pkPath) = c.PartitionKeyPath[1..].Replace('/', '.').SplitTwo('.');
                    var buffer = new BufferBlock<Document>(new () { BoundedCapacity = MaxDocCopyBufferSize });

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

                            messageChannel.TryWrite(new (c.Id, $"{(float)p/total:p0} ({p}/{total})"));
                        },
                        new () { MaxDegreeOfParallelism = MaxDocCopyParallel });

                    using (buffer.LinkTo(consumer, new() { PropagateCompletion = true }))
                    {
                        try
                        {
                            // Retrieve all documents
                            using var docFeed = sourceContainer.GetItemQueryStreamIterator();

                            // Producer
                            while (docFeed.HasMoreResults)
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

                messageChannel.TryWrite(new (c.Id, "[green]Done[/]"));
            }
            catch (CosmosException e)
            {
                messageChannel.TryWrite(new (c.Id, $"[red]Failed ({e.GetType().Name})[/]"));
            }
            catch (Exception)
            {
                messageChannel.TryWrite(new (c.Id, $"[red]Failed[/]"));
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