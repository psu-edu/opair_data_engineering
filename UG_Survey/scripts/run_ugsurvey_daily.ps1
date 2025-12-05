Param(
    [string]$EnvLabel = "DEV",
    [string]$Mode = "daily",
    [string]$DataFolder = "F:\Dat\UGSurvey",
    [string]$Pattern = "UGSurveyData_*.csv"
)

$ErrorActionPreference = "Stop"

# 1. Find the newest raw file
$rawFile = Get-ChildItem -Path $DataFolder -Filter $Pattern |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if (-not $rawFile) {
    Write-Error "No raw file found in $DataFolder matching $Pattern"
    exit 1
}

Write-Host "Using raw file:" $rawFile.FullName

# 2. Environment variables
$env:MSSQL_URL = "mssql+pyodbc://@s8-whse-sql-d01/ugsurvey?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"

# Email overrides (optional – can rely on settings.yaml if you prefer)
$env:UGS_EMAIL_FROM = "L-DWEMAIL@LISTS.PSU.EDU"
$env:UGS_EMAIL_TO   = "L-sarakaj@psu.edu"

# 3. Run full ETL
$pythonExe = "Z:\jjs7199\PY\UG_Survey\.venv\Scripts\python.exe"

& $pythonExe -m ug_survey.run_full_etl `
    --env $EnvLabel `
    --mode $Mode `
    --raw-file $rawFile.FullName

$exitCode = $LASTEXITCODE
Write-Host "ETL exit code:" $exitCode
exit $exitCode
