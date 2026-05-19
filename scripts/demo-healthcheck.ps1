<#
.SYNOPSIS
    TaaSim live-demo health check. Verifies that every component required
    for the Grafana dashboard is up and producing fresh data.

.DESCRIPTION
    Runs read-only checks against the running Docker stack and prints a
    PASS/FAIL summary. By default it does NOT destroy any volumes.

    Checks performed:
      1. docker compose config validity
      2. All required containers are healthy (or running for those without healthchecks)
      3. The 3 Flink jobs are RUNNING (GPS Normalizer, Demand Aggregator, Trip Matcher)
      4. Cassandra has fresh data:
           - vehicle_positions: latest event_time within --max-gps-age-seconds
           - demand_zones: latest window_start within --max-demand-age-seconds
           - trips: at least one row in today's UTC date_bucket
      5. Prints service URLs.

.PARAMETER MaxGpsAgeSeconds
    Acceptable age in seconds for the freshest vehicle_positions row. Default: 30.

.PARAMETER MaxDemandAgeSeconds
    Acceptable age in seconds for the freshest demand_zones row. Default: 90.

.PARAMETER Reset
    DANGEROUS. When supplied, the script will prompt for explicit confirmation
    and then run `docker compose down -v` to remove all volumes. Off by default.

.EXAMPLE
    .\scripts\demo-healthcheck.ps1

.EXAMPLE
    .\scripts\demo-healthcheck.ps1 -MaxGpsAgeSeconds 60
#>
[CmdletBinding()]
param(
    [int]$MaxGpsAgeSeconds = 30,
    [int]$MaxDemandAgeSeconds = 90,
    [switch]$Reset
)

$ErrorActionPreference = "Stop"
$script:Failures = @()
$script:Warnings = @()

function Write-Result {
    param([string]$Name, [string]$Status, [string]$Detail = "")
    $color = switch ($Status) {
        "PASS" { "Green" }
        "FAIL" { "Red" }
        "WARN" { "Yellow" }
        default { "Gray" }
    }
    Write-Host ("[{0}] {1}" -f $Status.PadRight(4), $Name) -ForegroundColor $color
    if ($Detail) {
        foreach ($line in ($Detail -split "`n")) {
            if ($line.Trim()) { Write-Host "       $line" -ForegroundColor DarkGray }
        }
    }
    if ($Status -eq "FAIL") { $script:Failures += $Name }
    if ($Status -eq "WARN") { $script:Warnings += $Name }
}

# ── -Reset guard (explicit, never default) ───────────────────────────────
if ($Reset) {
    Write-Host "WARNING: -Reset will delete ALL Docker volumes for this project." -ForegroundColor Yellow
    $answer = Read-Host "Type 'DESTROY' to continue, anything else to abort"
    if ($answer -ne "DESTROY") {
        Write-Host "Aborted." -ForegroundColor Yellow
        exit 1
    }
    docker compose down -v
    Write-Host "Volumes removed. Re-run without -Reset to bring the stack back up." -ForegroundColor Yellow
    exit 0
}

Write-Host ""
Write-Host "=== TaaSim Demo Healthcheck ===" -ForegroundColor Cyan
Write-Host ""

# 1. docker compose config validity
try {
    $null = docker compose config --quiet 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Result "docker compose config" "PASS"
    } else {
        Write-Result "docker compose config" "FAIL" "Compose file failed validation."
    }
} catch {
    Write-Result "docker compose config" "FAIL" $_.Exception.Message
}

# 2. Required containers
$required = @(
    "taasim-cassandra",
    "taasim-kafka",
    "taasim-kafka-connect",
    "taasim-kafka-ui",
    "taasim-flink-jm",
    "taasimm-flink-taskmanager-1",
    "taasim-grafana",
    "taasim-gps-producer",
    "taasim-trip-producer"
)
$psJson = docker ps --format "{{json .}}" | ForEach-Object { $_ | ConvertFrom-Json }
foreach ($name in $required) {
    $row = $psJson | Where-Object { $_.Names -eq $name }
    if (-not $row) {
        Write-Result "container ${name}" "FAIL" "Not running."
        continue
    }
    $statusText = $row.Status
    if ($statusText -match "unhealthy") {
        Write-Result "container ${name}" "FAIL" $statusText
    } elseif ($statusText -match "healthy|Up") {
        Write-Result "container ${name}" "PASS" $statusText
    } else {
        Write-Result "container ${name}" "WARN" $statusText
    }
}

# 3. Flink jobs
try {
    $jobsJson = docker exec taasim-flink-jm curl -s http://localhost:8081/jobs/overview 2>$null
    if (-not $jobsJson) {
        Write-Result "Flink jobs reachable" "FAIL" "Could not reach Flink REST API."
    } else {
        $jobs = ($jobsJson | ConvertFrom-Json).jobs
        $expected = @("GPS Normalizer", "Demand Aggregator", "Trip Matcher")
        foreach ($exp in $expected) {
            $j = $jobs | Where-Object { $_.name -like "*$exp*" }
            if (-not $j) {
                Write-Result "Flink job '$exp'" "FAIL" "Not found."
            } elseif ($j.state -eq "RUNNING") {
                Write-Result "Flink job '$exp'" "PASS" "state=RUNNING"
            } else {
                Write-Result "Flink job '$exp'" "FAIL" "state=$($j.state)"
            }
        }
    }
} catch {
    Write-Result "Flink jobs check" "FAIL" $_.Exception.Message
}

# 4. Cassandra freshness — query the busiest partition for vehicle_positions and
# the first zone for demand_zones. Avoids unbounded GROUP BY which times out.
function Get-CassandraValue {
    param([string]$Cql)
    $out = docker exec taasim-cassandra cqlsh -e $Cql --request-timeout=15 2>&1
    if ($LASTEXITCODE -ne 0) { return $null }
    # cqlsh tabular output (3 useful lines): header, separator, data row, blank, "(N rows)".
    # Filter out blank lines, separators (---), and the trailing row-count line.
    $lines = ($out -split "`n") |
        ForEach-Object { $_.TrimEnd("`r") } |
        Where-Object {
            $_.Trim() -ne "" -and
            $_ -notmatch "^\s*-+\s*$" -and
            $_ -notmatch "^\(\d+ rows?\)$"
        }
    if ($lines.Count -lt 2) { return $null }
    # lines[0] = header (column name), lines[1] = data row.
    return $lines[1].Trim()
}

function Parse-CassandraTimestamp {
    param([string]$Value)
    # cqlsh emits e.g. "2026-05-19 00:54:32.264000+0000" (offset without colon).
    # Normalise: replace single space with 'T' and convert "+HHMM" to "+HH:MM".
    $normalized = $Value -replace "\s+", "T"
    if ($normalized -match "([+-])(\d{2})(\d{2})$") {
        $normalized = $normalized -replace "([+-])(\d{2})(\d{2})$", '$1$2:$3'
    }
    return [datetimeoffset]::Parse($normalized, [Globalization.CultureInfo]::InvariantCulture)
}

# vehicle_positions freshness (zone 1 is the busiest in current data)
try {
    $cql = "SELECT event_time FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id=1 LIMIT 1;"
    $val = Get-CassandraValue -Cql $cql
    if (-not $val) {
        Write-Result "vehicle_positions freshness" "FAIL" "No rows returned for zone_id=1."
    } else {
        $dto = Parse-CassandraTimestamp -Value $val
        $ageSec = ((Get-Date).ToUniversalTime() - $dto.UtcDateTime).TotalSeconds
        if ($ageSec -le $MaxGpsAgeSeconds) {
            Write-Result "vehicle_positions freshness" "PASS" ("latest={0}, age={1:F1}s (<= {2}s)" -f $val, $ageSec, $MaxGpsAgeSeconds)
        } else {
            Write-Result "vehicle_positions freshness" "FAIL" ("latest={0}, age={1:F1}s (> {2}s)" -f $val, $ageSec, $MaxGpsAgeSeconds)
        }
    }
} catch {
    Write-Result "vehicle_positions freshness" "FAIL" $_.Exception.Message
}

# demand_zones freshness
try {
    $cql = "SELECT window_start FROM taasim.demand_zones WHERE city='casablanca' AND zone_id=1 LIMIT 1;"
    $val = Get-CassandraValue -Cql $cql
    if (-not $val) {
        Write-Result "demand_zones freshness" "FAIL" "No rows for zone_id=1."
    } else {
        $dto = Parse-CassandraTimestamp -Value $val
        $ageSec = ((Get-Date).ToUniversalTime() - $dto.UtcDateTime).TotalSeconds
        if ($ageSec -le $MaxDemandAgeSeconds) {
            Write-Result "demand_zones freshness" "PASS" ("latest={0}, age={1:F1}s (<= {2}s)" -f $val, $ageSec, $MaxDemandAgeSeconds)
        } else {
            Write-Result "demand_zones freshness" "FAIL" ("latest={0}, age={1:F1}s (> {2}s)" -f $val, $ageSec, $MaxDemandAgeSeconds)
        }
    }
} catch {
    Write-Result "demand_zones freshness" "FAIL" $_.Exception.Message
}

# trips: at least one row today (UTC)
try {
    $today = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
    $cql = "SELECT trip_id FROM taasim.trips WHERE city='casablanca' AND date_bucket='$today' LIMIT 1;"
    $val = Get-CassandraValue -Cql $cql
    if (-not $val) {
        Write-Result "trips today ($today)" "WARN" "No trips yet for today's date_bucket (matcher may not have produced any output yet)."
    } else {
        Write-Result "trips today ($today)" "PASS" "At least one trip in date_bucket=$today."
    }
} catch {
    Write-Result "trips today" "FAIL" $_.Exception.Message
}

# 5. URLs
Write-Host ""
Write-Host "=== Service URLs ===" -ForegroundColor Cyan
Write-Host "  Grafana       : http://localhost:3000  (admin / admin)" -ForegroundColor Gray
Write-Host "  Flink UI      : http://localhost:8081" -ForegroundColor Gray
Write-Host "  Kafka UI      : http://localhost:8090" -ForegroundColor Gray
Write-Host "  FastAPI       : http://localhost:8000  (if api service is up)" -ForegroundColor Gray
Write-Host "  Spark Master  : http://localhost:8080" -ForegroundColor Gray
Write-Host "  MinIO Console : http://localhost:9001" -ForegroundColor Gray
Write-Host ""

# Summary
Write-Host "=== Summary ===" -ForegroundColor Cyan
if ($script:Failures.Count -eq 0 -and $script:Warnings.Count -eq 0) {
    Write-Host "All checks passed." -ForegroundColor Green
    exit 0
}
if ($script:Failures.Count -eq 0) {
    Write-Host ("PASS with {0} warning(s):" -f $script:Warnings.Count) -ForegroundColor Yellow
    foreach ($w in $script:Warnings) { Write-Host "  - $w" -ForegroundColor Yellow }
    exit 0
}
Write-Host ("FAIL: {0} failure(s), {1} warning(s)." -f $script:Failures.Count, $script:Warnings.Count) -ForegroundColor Red
foreach ($f in $script:Failures) { Write-Host "  - $f" -ForegroundColor Red }
foreach ($w in $script:Warnings) { Write-Host "  - $w" -ForegroundColor Yellow }
exit 1
