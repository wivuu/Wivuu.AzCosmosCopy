param(
    [string]$key = ""
)

function publish ($path) {
    $files = Get-ChildItem $path `
        | Sort-Object LastWriteTime `
        | Select-Object -Last 2

    foreach ($file in $files) {
        Write-Host "Publish $file ..."

        if ($key) {
            dotnet nuget push $file --source "nuget" -k $key --skip-duplicate
        }
        else {
            dotnet nuget push $file --source "nuget" --skip-duplicate
        }
    }
}

dotnet pack -c Release
dotnet pack -c Release -p:AsLibrary=true 

publish .\nupkg\*.nupkg
