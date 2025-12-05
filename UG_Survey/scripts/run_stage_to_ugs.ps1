<#
.SYNOPSIS
    Wrapper for UG_Survey Python loader.

.DESCRIPTION
    - Activates venv
    - Sets MSSQL_URL env var for SQLAlchemy
    - Executes ug_survey.stage_to_ugs
    - Supports --replace-file and --force
#>





param(
    [string]$Settings = "config/settings.yaml"
)

# --- basic setup ---
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python      = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

# Use the same MSSQL_URL as run_load_raw.ps1
$env:MSSQL_URL = "mssql+pyodbc://@localhost/whsestage?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes&Encrypt=no"

Write-Host "=== Stage -> UGS split ==="
Write-Host "ProjectRoot : $ProjectRoot"
Write-Host "Python      : $Python"
Write-Host "Settings    : $Settings"
Write-Host "MSSQL_URL   : $env:MSSQL_URL"
Write-Host ""

& $Python -m ug_survey.stage_to_ugs --settings $Settings
$ExitCode = $LASTEXITCODE

Write-Host ""
Write-Host "Python exited with code $ExitCode"
exit $ExitCode
