param()

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $root

Write-Host "=== Meralco Bill Calculator Builder ===" -ForegroundColor Cyan

try {
    node build.js
    if ($LASTEXITCODE -eq 0) {
        $exe = Get-ChildItem -LiteralPath "dist" -Recurse -Filter "MeralcoBillCalculator.exe" | Select-Object -First 1
        Write-Host "`nBuild successful!" -ForegroundColor Green
        Write-Host "App: $($exe.FullName)" -ForegroundColor Green
    } else {
        throw "Build script failed"
    }
} catch {
    Write-Host "Build failed: $_" -ForegroundColor Red
    exit 1
}
