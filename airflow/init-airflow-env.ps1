$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (Test-Path ".env") {
    throw "airflow\.env already exists. It was not overwritten. Edit it manually if needed."
}
if (!(Test-Path ".env.example")) {
    throw "Missing airflow\.env.example"
}

function New-UrlSafeBase64([int]$ByteCount) {
    $bytes = New-Object byte[] $ByteCount
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return [Convert]::ToBase64String($bytes).Replace("+", "-").Replace("/", "_")
}

function New-HexSecret([int]$ByteCount) {
    $bytes = New-Object byte[] $ByteCount
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return ([BitConverter]::ToString($bytes)).Replace("-", "").ToLowerInvariant()
}

$content = Get-Content ".env.example" -Raw
$content = $content.Replace("REPLACE_WITH_GENERATED_FERNET_KEY", (New-UrlSafeBase64 32))
$content = $content.Replace("REPLACE_WITH_GENERATED_JWT_SECRET", (New-UrlSafeBase64 48))
$content = $content.Replace("REPLACE_WITH_GENERATED_HEX_PASSWORD", (New-HexSecret 24))
$content = $content.Replace("REPLACE_WITH_A_STRONG_ADMIN_PASSWORD", (New-HexSecret 16))
$envPath = Join-Path $PSScriptRoot ".env"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($envPath, $content, $utf8NoBom)

Write-Host "Created airflow\.env with randomly generated local secrets." -ForegroundColor Green
Write-Host "Now add DATA_GOV_API_KEY to airflow\.env locally. Never commit or share this file." -ForegroundColor Yellow
