using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Threading;
using Microsoft.Azure.Cosmos;
using Spectre.Console;

var root = new RootCommand
{
    new Option<string?>(
        new [] { "-s", "--source" }, "Source connection string (required)"
    ),

    new Option<string?>(
        new [] { "--sd", "--source-database" }, "Source database name (required)"
    ),
    
    new Option<string?>(
        new [] { "-d", "--destination" }, "Destination connection string"
    ),
    
    new Option<string?>(
        new [] { "--dd", "--destination-database" }, "Destination database name"
    ),
};

// Add validators
root.AddValidator(result => result.Children.Contains("-s") ? default : "--source is required");

root.AddValidator(result => result.Children.Contains("--sd") ? default : "--source-database is required");

root.AddValidator(result => 
    !result.Children.Contains("-d") && !result.Children.Contains("--dd")
    ? "--destination must be specified if --destination-database is not" : default
);

root.Description = "Copy Cosmos database from source to destination";

using var cancellation = new CancellationTokenSource();

// Handle command
root.Handler = CommandHandler.Create(
    async (Args args) =>
    {
        if (args.Destination is null && args.DestinationDatabase == args.SourceDatabase)
            throw new System.Exception("--destination cannot be copied to --source with the same --destination-database name");

        var options = new CosmosClientOptions 
        { 
            ConnectionMode     = ConnectionMode.Direct,
            AllowBulkExecution = true,
        };

        var sourceClient = new CosmosClient(args.Source, options);
        var destClient   = new CosmosClient(args.Destination ?? args.Source, options);

        var result = await DbCopier.CopyAsync(
            options: new(sourceClient, destClient, args.SourceDatabase, args.DestinationDatabase ?? args.SourceDatabase),
            cancellation.Token
        );

        return result ? 0 : 1;
    });

// Handle cancel
Console.CancelKeyPress += new ConsoleCancelEventHandler((sender, e) => 
{
    cancellation.Cancel();
    
    AnsiConsole.WriteLine("Cancellation requested");
});

await root.InvokeAsync(args);

class Args
{
    public string Source { get; init; } = default!;
    public string SourceDatabase { get; init; } = default!;
    public string? Destination { get; init; }
    public string? DestinationDatabase { get; init; }
}