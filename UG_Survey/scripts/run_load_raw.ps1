<#
.SYNOPSIS
    Wrapper for UG_Survey Python loader.

.DESCRIPTION
    - Activates venv
    - Sets MSSQL_URL env var for SQLAlchemy
    - Executes ug_survey.load_raw with logging
    - Supports --replace-file and --force
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$File,                     # Path to CSV file
    [switch]$ReplaceFile,              # Delete and reload
    [switch]$Force,                    # Force even if hash/name already loaded
    [string]$Settings = "config/settings.yaml"  # Path to settings YAML
)

# -----------------------------------------------------------------------------
# ENVIRONMENT SETUP
# -----------------------------------------------------------------------------
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$VenvPython  = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

# Confirm Python exists
if (-not (Test-Path $VenvPython)) {
    Write-Error "Virtual environment Python not found: $VenvPython"
    exit 1
}

# Build MSSQL connection URL (adjust driver or DB if needed)
$env:MSSQL_URL = "mssql+pyodbc://@localhost/whsestage?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes&Encrypt=no"

# -----------------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------------
$LogDir = Join-Path $ProjectRoot "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

$TimeStamp = (Get-Date -Format "yyyyMMdd_HHmmss")
$LogFile   = Join-Path $LogDir "load_raw_$TimeStamp.log"

# -----------------------------------------------------------------------------
# COMMAND BUILD
# -----------------------------------------------------------------------------
$ArgsList = @("-m", "ug_survey.load_raw", "--file", $File, "--settings", $Settings)

if ($ReplaceFile) { $ArgsList += "--replace-file" }
if ($Force)       { $ArgsList += "--force" }

# -----------------------------------------------------------------------------
# EXECUTE
# -----------------------------------------------------------------------------
Write-Host "============================================================"
Write-Host " UG Survey Loader Run - $TimeStamp"
Write-Host " File: $File"
Write-Host " ReplaceFile: $ReplaceFile  Force: $Force"
Write-Host " Log: $LogFile"
Write-Host "============================================================`n"

# Run Python and tee output to log
& $VenvPython @ArgsList 2>&1 | Tee-Object -FilePath $LogFile

$ExitCode = $LASTEXITCODE

if ($ExitCode -eq 0) {
    Write-Host "`n✅ UG_Survey load completed successfully. See $LogFile"
} else {
    Write-Host "`n❌ UG_Survey load failed with exit code $ExitCode. Check $LogFile for details."
}

exit $ExitCode
