﻿using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Threading;
using Wivuu.AzCosmosCopy;

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

    new Option<int?>(
        new [] { "--pc", "--parallel-containers" }, "Parallel container copies"
    ),
    
    new Option<int?>(
        new [] { "--pd", "--parallel-documents" }, "Parallel document copies"
    ),

    new Option(new [] { "-b", "--bulk" }, "Use bulk executor (serverless not supported)"),

    new Option<int?>(
        new [] { "--dbscale" }, "Destination database scale (serverless not supported)"
    ),
    
    new Option<int?>(
        new [] { "--dcscale" }, "Destination container scale (serverless not supported)"
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

        var copyOptions = new DbCopierOptions(
            args.Source,
            string.IsNullOrWhiteSpace(args.Destination) ? args.Source : args.Destination,
            args.SourceDatabase,
            args.DestinationDatabase ?? args.SourceDatabase
        )
        {
            MaxContainerParallel           = args.ParallelContainers,
            MaxDocCopyParallel             = args.ParallelDocuments,
            UseBulk                        = args.Bulk,
            DestinationContainerThroughput = args.DcScale,
            DestinationDbThroughput        = args.DbScale,
        };

        var activity = DbCopier.CopyAsync(copyOptions, cancellation.Token);
        var result   = await DbCopier.RenderCopyDetailsAsync(activity);

        return result ? 0 : 1;
    });

// Handle cancel
Console.CancelKeyPress += new ConsoleCancelEventHandler((sender, e) =>
{
    e.Cancel = true;
    cancellation.Cancel();
});

await root.InvokeAsync(args);

class Args
{
    public string Source { get; init; } = default!;
    public string SourceDatabase { get; init; } = default!;
    public string? Destination { get; init; }
    public string? DestinationDatabase { get; init; }
    public int ParallelContainers { get; set; } = 10;
    public int ParallelDocuments { get; set; } = 100;
    public bool Bulk { get; set; }
    public int? DbScale { get; set; }
    public int? DcScale { get; set; }
}