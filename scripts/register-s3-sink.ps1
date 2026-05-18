# =============================================================
# register-s3-sink.ps1 (legacy wrapper)
# The old unified S3 Sink connector is deprecated.
# This wrapper now calls the split connector registration script.
# =============================================================

Write-Host "NOTE: register-s3-sink.ps1 is legacy; using split connector registration." -ForegroundColor Yellow

$scriptPath = Join-Path $PSScriptRoot "register-connectors.ps1"
if (-not (Test-Path $scriptPath)) {
    Write-Error "Missing script: $scriptPath"
    exit 1
}

& $scriptPath
exit $LASTEXITCODE
