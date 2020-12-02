using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using Microsoft.Azure.Cosmos;

var root = new RootCommand
{
    new Option<string?>(
        new [] { "-s", "--source" }, "Source connection string"
    ),
    
    new Option<string?>(
        new [] { "-d", "--destination" }, "Destination connection string"
    ),

    new Option<string?>(
        new [] { "--sn", "--source-database" }, "Source database name"
    ),
    
    new Option<string?>(
        new [] { "--dn", "--destination-database" }, "Destination database name"
    ),
};

root.Description = "Copy Cosmos database from source to destination";

root.Handler = CommandHandler.Create(
    async (string source, string? destination, string sourceDatabase, string? destinationDatabase) =>
    {
        if (source is null)
        {
            Console.WriteLine("--source is required");
            return 1;
        }

        if (sourceDatabase is null)
        {
            Console.WriteLine("--source-database must be specified");
            return 2;
        }

        if (destination is null && destinationDatabase is null)
        {
            Console.WriteLine("--destination must be specified if --destination-database is not");
            return 3;
        }
        
        if (destination is null && destinationDatabase == sourceDatabase)
        {
            Console.WriteLine("--destination cannot be copied to --source with the same --destination-database name");
            return 4;
        }

        var options = new CosmosClientOptions 
        { 
            ConnectionMode     = ConnectionMode.Direct,
            AllowBulkExecution = true,
        };

        var sourceClient = new CosmosClient(source, options);
        var destClient   = new CosmosClient(destination ?? source, options);

        return await DbCopier.CopyAsync(sourceClient, destClient, sourceDatabase, destinationDatabase ?? sourceDatabase)
            ? 0 : 5;
    });

await root.InvokeAsync(args);
