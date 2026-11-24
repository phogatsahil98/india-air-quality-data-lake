$ErrorActionPreference = 'Stop'

$ProjectRoot = (Get-Location).Path
$StageRoot = Join-Path $env:TEMP 'india_aqi_source_review'
$Desktop = [Environment]::GetFolderPath('Desktop')
$ZipPath = Join-Path $Desktop 'india_aqi_source_review.zip'

if (-not (Test-Path (Join-Path $ProjectRoot 'airflow')) -or -not (Test-Path (Join-Path $ProjectRoot 'dbt'))) {
    throw 'Run this script from the india-air-quality-data-lake project root.'
}

$ExcludedDirectories = @(
    '.git', '.venv', '.venv-dbt', '.venv-lab', 'venv', 'env',
    'node_modules', '__pycache__', '.pytest_cache', '.mypy_cache', '.ruff_cache',
    'target', 'logs', 'data', 'bronze_cache', 'raw', 'processed',
    '.terraform', '.cache', '.aws', '.dbt'
)

$AllowedExtensions = @(
    '.py', '.sql', '.yml', '.yaml', '.md', '.txt', '.toml', '.cfg', '.ini',
    '.ps1', '.sh', '.properties'
)
$AllowedNames = @(
    '.gitignore', '.dockerignore', 'Dockerfile', 'Makefile',
    '.env.example', 'docker-compose.yaml', 'docker-compose.yml'
)

$ExcludedExactNames = @(
    '.env', 'profiles.yml', 'credentials', 'credentials.json',
    'id_rsa', 'id_ed25519'
)

if (Test-Path $StageRoot) {
    Remove-Item $StageRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $StageRoot -Force | Out-Null

$Included = [System.Collections.Generic.List[string]]::new()

function Copy-ReviewFiles {
    param([Parameter(Mandatory=$true)][string]$CurrentDirectory)

    foreach ($Item in Get-ChildItem -LiteralPath $CurrentDirectory -Force) {
        if ($Item.PSIsContainer) {
            if ($ExcludedDirectories -contains $Item.Name) { continue }
            # Exclude common generated/build directories wherever they occur.
            if ($Item.Name -match '^\.?(airflow-)?(logs|log|cache)$') { continue }
            Copy-ReviewFiles -CurrentDirectory $Item.FullName
            continue
        }

        if ($ExcludedExactNames -contains $Item.Name) { continue }
        if ($Item.Name -match '^\.env\.(?!example$)') { continue }
        if ($Item.Extension -in @('.zip', '.parquet', '.csv', '.json', '.db', '.sqlite', '.log', '.pyc', '.pem', '.key', '.p12', '.pfx')) { continue }
        if (($AllowedExtensions -notcontains $Item.Extension) -and ($AllowedNames -notcontains $Item.Name)) { continue }

        $RelativePath = [System.IO.Path]::GetRelativePath($ProjectRoot, $Item.FullName)
        $DestinationPath = Join-Path $StageRoot $RelativePath
        $DestinationDirectory = Split-Path -Parent $DestinationPath
        New-Item -ItemType Directory -Path $DestinationDirectory -Force | Out-Null
        Copy-Item -LiteralPath $Item.FullName -Destination $DestinationPath -Force
        $Included.Add($RelativePath)
    }
}

Copy-ReviewFiles -CurrentDirectory $ProjectRoot

if ($Included.Count -eq 0) {
    throw 'No source files were collected. Check that you ran the script from the project root.'
}

if (Test-Path $ZipPath) {
    Remove-Item $ZipPath -Force
}
Compress-Archive -Path (Join-Path $StageRoot '*') -DestinationPath $ZipPath -CompressionLevel Optimal

Write-Host ''
Write-Host 'Source review bundle created.' -ForegroundColor Green
Write-Host "Files included: $($Included.Count)"
Write-Host "Bundle: $ZipPath"
Write-Host ''
Write-Host 'Included source files:' -ForegroundColor Cyan
$Included | Sort-Object | ForEach-Object { Write-Host "  $_" }
Write-Host ''
Write-Host 'Excluded: virtual environments, .git, data files, Parquet/CSV/JSON, dbt target, logs, .env, AWS/dbt credentials, and ZIP bundles.' -ForegroundColor Yellow
Write-Host 'Before uploading, inspect the bundle and ensure no credentials or literal API keys appear in source code.' -ForegroundColor Yellow
