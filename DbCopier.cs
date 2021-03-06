using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Threading.Channels;
using System.Threading;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Spectre.Console;

namespace Wivuu.AzCosmosCopy
{
    public record DbCopierDestinationOptions
    (
        string Destination,
        string DestinationDatabase
    )
    {
        /// <summary>
        /// Cosmost client options
        /// </summary>
        public CosmosClientOptions ClientOptions { get; init; } = new CosmosClientOptions
        {
            ConnectionMode                        = ConnectionMode.Direct,
            AllowBulkExecution                    = true,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMinutes(5),
            MaxRetryAttemptsOnRateLimitedRequests = 60,
        };
        
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
        /// Execute N bulk copy operations per container
        /// </summary>
        public int MaxBulkCopyParallel { get; init; } = 10;

        /// <summary>
        /// Enqueue N documents per container copy operation
        /// </summary>
        public int MaxDocCopyBufferSize { get; init; } = 100;

        /// <summary>
        /// Enqueue N table display render updates (affects rendering only)
        /// </summary>
        public int UIRenderQueueCapacity { get; init; } = 5000;

        /// <summary>
        /// Whether or not to use bulk executor (serverless not supported)
        /// </summary>
        public bool UseBulk { get; set; }
        
        /// <summary>
        /// Throughput for newly created containers
        /// </summary>
        public int? DestinationContainerThroughput { get; set; }

        /// <summary>
        /// Throughput for newly created databases
        /// </summary>
        public int? DestinationDbThroughput { get; set; }
        
        internal (Uri uri, string key) GetDestinationComponents()
        {
            var pattern = new Regex(@"AccountEndpoint=(?<uri>[^;]+);AccountKey=(?<key>[^;]+)");

            if (pattern.Match(Destination) is var match && match.Success)
            {
                return (new Uri(match.Groups["uri"].Value), match.Groups["key"].Value);
            }

            throw new Exception("Unable to parse input destination connection string");
        }
    }

    public record DbCopierOptions
    (
        string Source,
        string Destination,
        string SourceDatabase,
        string DestinationDatabase
    ) : DbCopierDestinationOptions(Destination, DestinationDatabase);

    public class DbCopier
    {
        public abstract record CopyDiagnostic(string container);
        public record CopyDiagnosticMessage(string container, string message, bool warning = false) : CopyDiagnostic(container);
        public record CopyDiagnosticProgress(string container, int progress, int total) : CopyDiagnostic(container);
        public record CopyDiagnosticDone(string container) : CopyDiagnostic(container);
        public record CopyDiagnosticFailed(string container, Exception exception) : CopyDiagnostic(container);

        /// <summary>
        /// Render activity from input stream
        /// </summary>
        public static async Task<bool> RenderCopyDetailsAsync(IAsyncEnumerable<CopyDiagnostic> activity)
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
                        var tasks = new Dictionary<string, ProgressTask>();

                        await foreach (var diag in activity)
                        {
                            if (string.IsNullOrEmpty(diag.container))
                            {
                                switch (diag)
                                {
                                    case CopyDiagnosticMessage(var container, var message, var warning):
                                        AnsiConsole.MarkupLine(
                                            (warning ? $"[yellow]{Markup.Escape(message)}[/]" : Markup.Escape(message))
                                        );
                                        continue;

                                    case CopyDiagnosticFailed(var container, var e):
                                        if (container is null)
                                        {
                                            AnsiConsole.MarkupLine(e switch
                                            {
                                                TaskCanceledException => "[yellow]Cancelled[/]",
                                                _                     => $"[red]Failed ({Markup.Escape(e.Message)})[/]",
                                            });
                                            continue;
                                        }
                                        break;

                                    default:
                                        throw new NotSupportedException("Container name must be specified");
                                }
                            }
                            else
                            {
                                if (!tasks.TryGetValue(diag.container, out var task))
                                {
                                    var taskName = diag.container;

                                    tasks.Add(taskName, task = ctx.AddTask(taskName, new () { MaxValue = 1 }));
                                }

                                switch (diag)
                                {
                                    case CopyDiagnosticProgress(var container, int progress, int total):
                                        var lastAmount = task.Value * total;
                                        task.Increment((progress - lastAmount) / total);
                                        break;

                                    case CopyDiagnosticMessage(var container, var message, var warning):
                                        AnsiConsole.MarkupLine(
                                            $"{Markup.Escape(container + " - ")}" +
                                            (warning ? $"[yellow]{Markup.Escape(message)}[/]" : Markup.Escape(message))
                                        );
                                        break;

                                    case CopyDiagnosticDone(var container):
                                        task.Increment(1);
                                        task.StopTask();

                                        AnsiConsole.MarkupLine(
                                            $"{Markup.Escape(container + " - ")}" +
                                            $"[green]Done ({task.ElapsedTime?.TotalSeconds:#,0.###}s)[/]"
                                        );
                                        break;

                                    case CopyDiagnosticFailed(var container, var e):
                                        task.StopTask();

                                        AnsiConsole.MarkupLine($"{Markup.Escape(container + " - ")}" + e switch
                                        {
                                            TaskCanceledException => "[yellow]Cancelled[/]",
                                            _                     => $"[red]Failed ({Markup.Escape(e.Message)})[/]",
                                        });
                                        break;
                                }
                            }
                        }
                    });

                return true;
            }
            catch (TaskCanceledException)
            {
                AnsiConsole.MarkupLine("[yellow]Copy cancelled[/]");
                return false;
            }
            catch (Exception error)
            {
                AnsiConsole.WriteException(error);
                return false;
            }
        }

        /// <summary>
        /// Copy from input source to destination
        /// </summary>
        public static IAsyncEnumerable<CopyDiagnostic> CopyAsync(
            DbCopierOptions options,
            CancellationToken cancellationToken = default)
        {
            var sourceDb = new CosmosClient(options.Source, options.ClientOptions)
                .GetDatabase(options.SourceDatabase);

            if (options.UseBulk)
            {
                var containersWithDocs = 
                    from c in GetSourceContainers(sourceDb, cancellationToken)
                    select new CopyDocumentStream<string>(c, GetDocumentsAsStrings(sourceDb, c.Properties.Id, null, cancellationToken));

                return CopyAsync(containersWithDocs, options, cancellationToken);
            }
            else
            {
                var containersWithDocs = 
                    from c in GetSourceContainers(sourceDb, cancellationToken)
                    select new CopyDocumentStream<object>(c, GetDocuments(sourceDb, c.Properties.Id, null, cancellationToken));

                return CopyAsync(containersWithDocs, options, cancellationToken);
            }
        }

        /// <summary>
        /// Copy from input container document info to destination
        /// </summary>
        public static IAsyncEnumerable<CopyDiagnostic> CopyAsync<T>(
            CopyContainerInfo ContainerInfo,
            IAsyncEnumerable<T> Documents,
            DbCopierDestinationOptions options,
            CancellationToken cancellationToken = default)
        {
            return CopyAsync(CopySingle(), options, cancellationToken);

            async IAsyncEnumerable<CopyDocumentStream<T>> CopySingle()
            {
                await Task.Yield();

                yield return new CopyDocumentStream<T>(ContainerInfo, Documents);
            }
        }

        /// <summary>
        /// Copy from input container document info to destination
        /// </summary>
        public static async IAsyncEnumerable<CopyDiagnostic> CopyAsync<T>(
            IAsyncEnumerable<CopyDocumentStream<T>> allContainers,
            DbCopierDestinationOptions options,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var (dest, destinationDatabase) = options;
            var destClient = new CosmosClient(dest, options.ClientOptions);

            var diagnostics = Channel.CreateBounded<CopyDiagnostic>(
                new BoundedChannelOptions(options.UIRenderQueueCapacity)
                {
                    FullMode = BoundedChannelFullMode.DropOldest
                });

            var diag = diagnostics.Writer;

            // Execute copy
            var copyTask = CopyDataPipeline();

            // For every message in the channel, yield back
            await foreach (var item in diagnostics.Reader.ReadAllAsync())
                yield return item;

            await copyTask;

            // Pipeline which copies all containers
            async Task CopyDataPipeline()
            {
                try
                {
                    var throughput = options.DestinationDbThroughput.HasValue
                        ? ThroughputProperties.CreateAutoscaleThroughput(options.DestinationDbThroughput.Value switch
                        {
                            < 4000 => 4000,
                            var n => n
                        })
                        : null;

                    await destClient.CreateDatabaseAsync(destinationDatabase, throughput);
                }
                catch (CosmosException)
                {
                    diag.TryWrite(new CopyDiagnosticMessage("", "Unable to create destination database - may already be copied", warning: true));
                }

                if (!options.UseBulk && typeof(T) == typeof(string))
                    diag.TryWrite(new CopyDiagnosticMessage("", "Without bulk enabled, strings must be deserialized anyway", warning: true));

                var buffer = new BufferBlock<CopyDocumentStream<T>>(new ()
                {
                    BoundedCapacity   = Math.Max(options.MaxContainerBufferSize, options.MaxContainerParallel),
                    CancellationToken = cancellationToken
                });

                var action = new ActionBlock<CopyDocumentStream<T>>(
                    options.UseBulk
                        ? CopyBulkFactory
                        : CopyStandardFactory, 
                    new ()
                    {
                        MaxDegreeOfParallelism    = options.MaxContainerParallel,
                        SingleProducerConstrained = true,
                        CancellationToken         = cancellationToken,
                    });

                using (buffer.LinkTo(action, new() { PropagateCompletion = true }))
                {
                    try
                    {
                        await foreach (var c in allContainers)
                        {
                            diag.TryWrite(new CopyDiagnosticMessage(c.ContainerInfo.Properties.Id, "Queued..."));

                            await buffer.SendAsync(c, cancellationToken);
                        }
                    }
                    finally
                    {
                        buffer.Complete();

                        try
                        {
                            await action.Completion;
                        }
                        finally
                        {
                            diag.Complete();
                        }
                    }
                }
            }

            // Pipeline which copies a single container using the standard Cosmos API
            async Task CopyStandardFactory(CopyDocumentStream<T> info)
            {
                var (container, allDocs) = info;
                var properties = container.Properties;
                var total = container.NumMessages;

                // Create dest container w/o indexing
                await using var containerCompletion = await CreateContainer(properties);

                if (total == null || total > 0)
                {
                    try
                    {
                        diag!.TryWrite(new CopyDiagnosticMessage(properties.Id, "Copying data..."));

                        var processed = 0;

                        var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };

                        var producer = new BufferBlock<object>(new ()
                        {
                            BoundedCapacity   = Math.Max(options.MaxDocCopyBufferSize, options.MaxDocCopyParallel),
                            CancellationToken = cancellationToken
                        });

                        var requestOptions = new ItemRequestOptions { EnableContentResponseOnWrite = false };

                        // Consumer pipeline
                        var consumer = new ActionBlock<object>(
                            async item =>
                            {
                                await containerCompletion.Container.UpsertItemAsync(item, requestOptions: requestOptions);

                                var progress = Interlocked.Increment(ref processed);

                                diag!.TryWrite(new CopyDiagnosticProgress(properties.Id, progress, total ?? progress));
                            },
                            new ()
                            {
                                MaxDegreeOfParallelism    = options.MaxDocCopyParallel,
                                SingleProducerConstrained = true,
                                CancellationToken         = cancellationToken
                            });

                        if (typeof(T) == typeof(string))
                        {
                            var deserialize = new TransformBlock<object, object>(
                                item => Newtonsoft.Json.JsonConvert.DeserializeObject(item as string),
                                new ()
                                {
                                    SingleProducerConstrained = true,
                                    CancellationToken = cancellationToken,
                                });

                            producer.LinkTo(deserialize, linkOptions);
                            deserialize.LinkTo(consumer, linkOptions);
                        }
                        else
                            producer.LinkTo(consumer, linkOptions);

                        try
                        {
                            // Retrieve all documents from producer
                            await foreach (var doc in allDocs)
                                await producer.SendAsync(doc!, cancellationToken);
                        }
                        finally
                        {
                            producer.Complete();

                            await consumer.Completion;
                        }

                        diag.TryWrite(new CopyDiagnosticDone(properties.Id));
                    }
                    catch (Exception e)
                    {
                        diag!.TryWrite(new CopyDiagnosticFailed(properties.Id, e));
                    }
                }
            };
        
            // Pipeline which copies a single container using the Bulk Cosmos API
            async Task CopyBulkFactory(CopyDocumentStream<T> info)
            {
                var (container, allDocs) = info;
                var properties = container.Properties;
                var total = container.NumMessages;

                // Create dest container w/o indexing
                await using var containerCompletion = await CreateContainer(properties);

                if (total == null || total > 0)
                {
                    try
                    {
                        diag!.TryWrite(new CopyDiagnosticMessage(properties.Id, "Copying data..."));

                        // Set up document client
                        var (uri, key) = options.GetDestinationComponents();
                        var policy = new Microsoft.Azure.Documents.Client.ConnectionPolicy
                        {
                            ConnectionMode = options.ClientOptions.ConnectionMode == ConnectionMode.Direct 
                                ? Microsoft.Azure.Documents.Client.ConnectionMode.Direct 
                                : Microsoft.Azure.Documents.Client.ConnectionMode.Gateway,
                            ConnectionProtocol = options.ClientOptions.ConnectionMode == ConnectionMode.Direct 
                                ? Microsoft.Azure.Documents.Client.Protocol.Tcp
                                : Microsoft.Azure.Documents.Client.Protocol.Https,
                        };

                        using var destDocClient = new Microsoft.Azure.Documents.Client.DocumentClient(uri, key, policy);
                        var destCollection = await destDocClient.ReadDocumentCollectionAsync(
                            Microsoft.Azure.Documents.Client.UriFactory.CreateDocumentCollectionUri(destinationDatabase, properties.Id));

                        var processed = 0;

                        // Create bulk executor
                        var bulkExecutor = new BulkExecutor(destDocClient, destCollection);
                        await bulkExecutor.InitializeAsync();

                        // Set retries to 0 to pass complete control to bulk executor.
                        destDocClient.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
                        destDocClient.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

                        var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };

                        // Consumer pipeline
                        var buffer = new BufferBlock<object>(new ()
                        {
                            BoundedCapacity   = Math.Max(options.MaxDocCopyBufferSize, options.MaxDocCopyParallel),
                            CancellationToken = cancellationToken
                        });

                        var batch = new BatchBlock<object>(
                            options.MaxDocCopyParallel,
                            new ()
                            {
                                CancellationToken = cancellationToken,
                                Greedy = true,
                            });

                        var consumer = new ActionBlock<object[]>(
                            async items =>
                            {
                                var response = await bulkExecutor.BulkImportAsync(
                                    documents: items,
                                    enableUpsert: true,
                                    disableAutomaticIdGeneration: true,
                                    maxConcurrencyPerPartitionKeyRange: null,
                                    maxInMemorySortingBatchSize: null,
                                    cancellationToken: cancellationToken
                                );

                                var progress = Interlocked.Add(ref processed, (int)response.NumberOfDocumentsImported);
                                diag.TryWrite(new CopyDiagnosticProgress(properties.Id, progress, total ?? progress));

                                // TODO: Handle other scenarios?
                                // while (response.NumberOfDocumentsImported < items.Length);
                            },
                            new ()
                            {
                                MaxDegreeOfParallelism    = options.MaxBulkCopyParallel,
                                SingleProducerConstrained = true,
                                CancellationToken         = cancellationToken
                            });

                        if (typeof(T) != typeof(string))
                        {
                            var serializer = new TransformBlock<object, string>(
                                data => Newtonsoft.Json.JsonConvert.SerializeObject(data),
                                new ()
                                {
                                    SingleProducerConstrained = true,
                                    CancellationToken = cancellationToken,
                                }
                            );

                            buffer.LinkTo(serializer, linkOptions);
                            serializer.LinkTo(batch, linkOptions);
                        }
                        else
                            buffer.LinkTo(batch, linkOptions);

                        batch.LinkTo(consumer, linkOptions);

                        try
                        {
                            // Retrieve all documents from producer
                            await foreach (var doc in allDocs)
                                await buffer.SendAsync(doc!, cancellationToken);
                        }
                        finally
                        {
                            buffer.Complete();

                            await consumer.Completion;
                        }

                        diag.TryWrite(new CopyDiagnosticDone(properties.Id));
                    }
                    catch (Exception e)
                    {
                        diag!.TryWrite(new CopyDiagnosticFailed(properties.Id, e));
                    }
                }
            };

            // Ensure container exists, if not create without index
            async Task<ContainerCompletion> CreateContainer(ContainerProperties c)
            {
                // Disable indexing
                var index = c.IndexingPolicy;
                var ttl = c.DefaultTimeToLive;

                c.DefaultTimeToLive = null;
                c.IndexingPolicy = new IndexingPolicy
                {
                    IndexingMode = IndexingMode.None,
                    Automatic    = false
                };

                // Setup throughput
                var throughput = options.DestinationContainerThroughput.HasValue
                    ? ThroughputProperties.CreateAutoscaleThroughput(options.DestinationContainerThroughput.Value switch
                    {
                        < 4000 => 4000,
                        int v => v
                    })
                    : null;

                // Create dest container
                var created = await destClient!.GetDatabase(destinationDatabase).CreateContainerIfNotExistsAsync(c, throughput) switch
                {
                    { StatusCode: System.Net.HttpStatusCode.Created } => true,
                    _ => false
                };

                var container = destClient.GetContainer(destinationDatabase, c.Id);

                return new(
                    container,
                    async () => 
                    {
                        if (created)
                        {
                            // Enable index
                            c.IndexingPolicy = index;
                            c.DefaultTimeToLive = ttl;

                            await container.ReplaceContainerAsync(c);
                        }
                    }
                );
            }
        }

        /// <summary>
        /// Retrieve all containers from the input database
        /// </summary>
        public static async IAsyncEnumerable<CopyContainerInfo> GetSourceContainers(
            Database sourceDb,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Gather list of containers to copy
            using var feed = sourceDb.GetContainerQueryIterator<ContainerProperties>();

            while (feed.HasMoreResults && !cancellationToken.IsCancellationRequested)
            {
                foreach (var properties in await feed.ReadNextAsync())
                {
                    var sourceContainer = sourceDb.GetContainer(properties.Id);
                    var total = await sourceContainer.GetItemLinqQueryable<object>(true).CountAsync();

                    yield return new CopyContainerInfo(
                        Properties: properties,
                        NumMessages: total
                    );
                }
            }
        }

        /// <summary>
        /// Retrieve all documents from the input container
        /// </summary>
        public static async IAsyncEnumerable<object> GetDocuments(
            Database database,
            string containerName,
            string? queryText = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Retrieve all documents
            var container = database.GetContainer(containerName);
            using var docFeed = queryText is not null
                ? container.GetItemQueryStreamIterator(queryDefinition: new (queryText))
                : container.GetItemQueryStreamIterator();

            var serializer = Newtonsoft.Json.JsonSerializer.CreateDefault();

            // Producer
            while (docFeed.HasMoreResults && !cancellationToken.IsCancellationRequested)
            {
                using var response = await docFeed.ReadNextAsync();

                using var sr = new StreamReader(response.Content);
                using var jr = new Newtonsoft.Json.JsonTextReader(sr);

                var all = serializer.Deserialize<DocumentContainer<object>>(jr);

                // Yield back all documents found
                for (var i = 0; i < all!.Documents.Count; ++i)
                    yield return all.Documents[i];
            }
        }
        
        /// <summary>
        /// Retrieve all documents from the input container as strings
        /// </summary>
        public static async IAsyncEnumerable<string> GetDocumentsAsStrings(
            Database database,
            string containerName,
            string? queryText = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Retrieve all documents
            var container = database.GetContainer(containerName);
            using var docFeed = queryText is not null
                ? container.GetItemQueryStreamIterator(queryDefinition: new (queryText))
                : container.GetItemQueryStreamIterator();
            
            // Producer
            while (docFeed.HasMoreResults && !cancellationToken.IsCancellationRequested)
            {
                using var response = await docFeed.ReadNextAsync();

                response.EnsureSuccessStatusCode();

                var all = await System.Text.Json.JsonSerializer.DeserializeAsync<DocumentContainer<System.Text.Json.JsonElement>>(
                    response.Content
                );

                // Yield back all documents found
                for (var i = 0; i < all!.Documents.Count; ++i)
                {
                    if (all.Documents[i].ToString() is string docString)
                        yield return docString;
                }
            }
        }

        class ContainerCompletion : System.IAsyncDisposable
        {
            public ContainerCompletion(Container container, Func<Task> completion)
            {
                Container  = container;
                Completion = completion;
            }

            public Container Container { get; }
            public Func<Task> Completion { get; }

            public async ValueTask DisposeAsync() => await Completion();
        }
    }

    /// <summary>
    /// Class which informs copier pipeline what data to copy
    /// </summary>
    public record CopyContainerInfo(
        ContainerProperties Properties,
        int? NumMessages = null
    );

    public record CopyDocumentStream<T>(
        CopyContainerInfo ContainerInfo,
        IAsyncEnumerable<T> DocumentProducer
    );

    /// <summary>
    /// Document response container
    /// </summary>
    public class DocumentContainer<T>
    {
        public List<T> Documents { get; set; } = default!;
    }
}