# Get current script directory
$scriptPath = $MyInvocation.MyCommand.Path
$scriptDir = Split-Path -Path $scriptPath -Parent

# Go up two levels to find the solution folder
$solutionDir = Resolve-Path (Join-Path $scriptDir '..\..')

try {
    # Find the .sln file
    $slnFile = Get-ChildItem -Path $solutionDir -Filter *.sln | Select-Object -First 1
    if (-not $slnFile) {
        throw "No solution (.sln) file found in $solutionDir"
    }

    # Parse .sln file to include only valid .csproj paths
    $slnContent = Get-Content $slnFile.FullName
    $projectPaths = $slnContent |
        Where-Object { $_ -match '^Project\(' -and $_ -match '\.csproj"' } |
        ForEach-Object {
            if ($_ -match '"(.*?)", "(.*?)",') {
                $relativePath = $Matches[2] -replace '\\\\', '\'
                [PSCustomObject]@{
                    Name = [System.IO.Path]::GetFileNameWithoutExtension($relativePath)
                    Path = (Resolve-Path (Join-Path $solutionDir $relativePath | Split-Path)).Path
                }
            }
        } | Sort-Object Path -Unique

    # Functions to remove comments
    function Remove-CodeComments {
        param ([string]$codeLine)
        $codeLine -replace "//.*", ""
    }

    function Remove-MultiLineComments {
        param ([string[]]$codeLines)
        $inCommentBlock = $false
        $cleanedLines = @()

        foreach ($line in $codeLines) {
            if ($inCommentBlock) {
                if ($line -match "\*/") {
                    $line = $line -replace ".*?\*/", ""
                    $inCommentBlock = $false
                } else {
                    continue
                }
            }

            if (-not $inCommentBlock) {
                if ($line -match "/\*") {
                    $line = $line -replace "/\*.*", ""
                    $inCommentBlock = $true
                }
            }

            if (-not $inCommentBlock -and $line.Trim() -ne "") {
                $cleanedLines += $line
            }
        }

        return $cleanedLines
    }

    # Process each project
    foreach ($project in $projectPaths) {
        Write-Host "Processing project: $($project.Path)"
        $outputFile = Join-Path -Path $scriptDir -ChildPath ("$($project.Name).txt")

        # Remove the output file if it exists
        if (Test-Path $outputFile) {
            Remove-Item $outputFile -Force
        }

        if (Test-Path $project.Path) {
            $csFiles = Get-ChildItem -Path $project.Path -Filter *.cs -Recurse -ErrorAction SilentlyContinue

            foreach ($file in $csFiles) {
                try {
                    Add-Content -Path $outputFile -Value "`n// File: $($file.FullName)`n"
                    $content = Get-Content -Path $file.FullName
                    $content = Remove-MultiLineComments -codeLines $content
                    $content = $content | ForEach-Object { Remove-CodeComments $_ }
                    $content | Add-Content -Path $outputFile
                } catch {
                    Write-Warning "Failed to process C# file: $($file.FullName)"
                }
            }

            $jsonFiles = Get-ChildItem -Path $project.Path -Recurse -Include "appsettings.json", "appsettings.*.json" -File -ErrorAction SilentlyContinue

            foreach ($jsonFile in $jsonFiles) {
                try {
                    Add-Content -Path $outputFile -Value "`n// File: $($jsonFile.FullName)`n"
                    Get-Content -Path $jsonFile.FullName | Add-Content -Path $outputFile
                } catch {
                    Write-Warning "Failed to process JSON file: $($jsonFile.FullName)"
                }
            }
        } else {
            Write-Warning "Project path not found: $($project.Path)"
        }
    }

    Write-Host "All projects processed. One .txt file per project created in: $scriptDir"
} catch {
    Write-Error $_.Exception.Message
}

Write-Host "Press any key to exit..."
[System.Console]::ReadKey() | Out-Null
