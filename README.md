# AzCosmosCopy

[![wivuu.azcosmoscopy](https://img.shields.io/nuget/v/azcosmoscopy.svg?label=azcosmoscopy)](https://www.nuget.org/packages/AzCosmosCopy/)


Simple CLI application which copies azure cosmos DB database to same or different cosmos account.

![](./sample.png)

## Usage

Install new
```sh
dotnet tool install -g azcosmoscopy
```

Upgrade to latest
```sh
dotnet tool update -g azcosmoscopy
```

Command line
```
Options:
  -s, --source <source>                                  Source connection string (required)
  --sd, --source-database <source-database>              Source database name (required)    
  -d, --destination <destination>                        Destination connection string      
  --dd, --destination-database <destination-database>    Destination database name
  -m, --minimal                                          Output minimal information
  --version                                              Show version information
  -?, -h, --help                                         Show help and usage information   
```


## Coming soon
- Support for tweaking concurrency parameters to not overwhelm low RU containers