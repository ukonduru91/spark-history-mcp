<#
.SYNOPSIS
    Start a local Spark History Server in Docker Desktop for testing the MCP.

.DESCRIPTION
    PowerShell equivalent of start_local_spark_history.sh, for Windows hosts
    running Docker Desktop. Reads the sample event logs from examples/basic.

.EXAMPLE
    .\start_local_spark_history.ps1
    .\start_local_spark_history.ps1 -Port 18080 -SparkVersion 3.5.5
#>
param(
    [string]$SparkVersion = "3.5.5",
    [string]$EventDir = "examples/basic",
    [string]$ContainerName = "spark-history-server",
    [int]$Port = 18080,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

Write-Host "Starting Local Spark History Server for MCP Testing"
Write-Host "==================================================="

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker is not running. Please start Docker Desktop first."
    exit 1
}

$eventPath = Join-Path (Get-Location) $EventDir
if (-not (Test-Path (Join-Path $eventPath "events"))) {
    Write-Error "Test data directory '$EventDir/events' not found."
    exit 1
}
if (-not (Test-Path (Join-Path $eventPath "history-server.conf"))) {
    Write-Error "Configuration file '$EventDir/history-server.conf' not found."
    exit 1
}

Write-Host "Stopping any existing container named $ContainerName..."
docker stop $ContainerName *> $null
docker rm $ContainerName *> $null

Write-Host ""
Write-Host "Event directories found:"
Get-ChildItem (Join-Path $eventPath "events") | ForEach-Object { Write-Host "   $($_.Name)" }

Write-Host ""
Write-Host "Web UI: http://localhost:$Port"
Write-Host "API:    http://localhost:$Port/api/v1/"
Write-Host "It may take 30-60 seconds for the server to fully start."
Write-Host ""

if ($DryRun) {
    Write-Host "Dry run completed successfully. All prerequisites are met."
    exit 0
}

docker run --rm `
    --name $ContainerName `
    --label "mcp-spark-test=true" `
    -v "${eventPath}:/mnt/data" `
    -p "${Port}:18080" `
    docker.io/apache/spark:$SparkVersion `
    /opt/java/openjdk/bin/java `
    -cp '/opt/spark/conf:/opt/spark/jars/*' `
    -Xmx1g `
    org.apache.spark.deploy.history.HistoryServer `
    --properties-file /mnt/data/history-server.conf
