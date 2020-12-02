param(
    [string]$key = ""
)

function publish ($path) {
    $file = Get-ChildItem $path `
        | Sort-Object LastWriteTime `
        | Select-Object -Last 1

    Write-Host "Publish $file ..."

    if ($key) {
        dotnet nuget push $file --source "nuget" -k $key --skip-duplicate
    }
    else {
        dotnet nuget push $file --source "nuget" --skip-duplicate
    }
}

dotnet pack --configuration Release

publish .\nupkg\*.nupkg
