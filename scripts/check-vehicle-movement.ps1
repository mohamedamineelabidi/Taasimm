#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Verify TaaSim vehicle_positions show realistic road-snapped movement.

.DESCRIPTION
    Read-only sanity check against the live Cassandra table:
      1. Same taxi has distinct lat/lon across its recent pings.
      2. status=moving rows have speed >= 1 km/h.
      3. status=idle/pickup/dropoff rows have low speed.
      4. snap_dist_m <= MaxSnapDistM (default 100).
      5. All latest points are inside the Casablanca bounding box.

.PARAMETER MaxSnapDistM
    Maximum acceptable snap distance in meters for a row to pass check (4).

.PARAMETER ZoneId
    Which Cassandra partition to sample for the per-taxi movement check.
#>

param(
    [double]$MaxSnapDistM = 100.0,
    [int]$ZoneId = 1
)

$ErrorActionPreference = "Stop"

# Casablanca bbox (must match the Flink validator bounds in gps_normalizer.py)
$LAT_MIN = 33.450; $LAT_MAX = 33.680
$LON_MIN = -7.720; $LON_MAX = -7.480

function Write-Result {
    param([string]$Status, [string]$Message)
    $color = switch ($Status) {
        "PASS" { "Green" }
        "WARN" { "Yellow" }
        default { "Red" }
    }
    Write-Host ("  [{0}] {1}" -f $Status, $Message) -ForegroundColor $color
}

function Invoke-Cql {
    param([string]$Cql)
    return (docker exec taasim-cassandra cqlsh -e $Cql --request-timeout=15 2>$null)
}

function Get-DataRows {
    param([string[]]$Lines)
    # cqlsh: blank, header, ----, data..., blank, "(N rows)"
    $data = @()
    foreach ($l in $Lines) {
        if ([string]::IsNullOrWhiteSpace($l)) { continue }
        $t = $l.Trim()
        if ($t -match '^-+(\s*\+\s*-+)*$') { continue }
        if ($t -match '^\(\d+\s+rows?\)$') { continue }
        $data += $l
    }
    if ($data.Count -lt 2) { return @() }
    return $data[1..($data.Count - 1)]
}

$exit = 0

Write-Host "=== Vehicle Movement Quality Check ===" -ForegroundColor Cyan
Write-Host ("Zone={0}  MaxSnapDistM={1}" -f $ZoneId, $MaxSnapDistM)

# ── 1. Same taxi moves over time ─────────────────────────────
Write-Host "`n[1] Per-taxi movement (lat/lon must change across pings)" -ForegroundColor Cyan
$cql1 = "SELECT taxi_id, lat, lon, event_time FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id=$ZoneId LIMIT 200;"
$raw1 = Invoke-Cql $cql1
$rows1 = Get-DataRows $raw1
if ($rows1.Count -lt 10) {
    Write-Result "WARN" "Only $($rows1.Count) recent rows for zone=$ZoneId — start the stack and wait a minute."
    $exit = 1
} else {
    $byTaxi = @{}
    foreach ($r in $rows1) {
        $cols = $r -split '\|' | ForEach-Object { $_.Trim() }
        if ($cols.Count -lt 4) { continue }
        $tid = $cols[0]; $lat = [double]$cols[1]; $lon = [double]$cols[2]
        if (-not $byTaxi.ContainsKey($tid)) { $byTaxi[$tid] = New-Object System.Collections.Generic.List[string] }
        $byTaxi[$tid].Add(("{0:N6},{1:N6}" -f $lat, $lon))
    }
    $multi = $byTaxi.GetEnumerator() | Where-Object { $_.Value.Count -ge 2 }
    if (-not $multi) {
        Write-Result "WARN" "No taxi has >=2 pings in the latest 200 rows — try increasing fleet activity."
    } else {
        $moving = 0; $stuck = 0
        foreach ($t in $multi) {
            $unique = ($t.Value | Select-Object -Unique).Count
            if ($unique -ge 2) { $moving++ } else { $stuck++ }
        }
        $total = $moving + $stuck
        $pct = if ($total -gt 0) { [int](100.0 * $moving / $total) } else { 0 }
        if ($pct -ge 80) {
            Write-Result "PASS" "$moving/$total taxis with multiple pings show distinct positions ($($pct) percent)."
        } else {
            Write-Result "FAIL" "Only $($pct) percent of taxis change position across pings ($moving/$total). Likely stuck on centroid+jitter."
            $exit = 1
        }
    }
}

# ── 2. status=moving has speed > 1 ───────────────────────────
Write-Host "`n[2] status=moving rows must have speed >= 1 km/h" -ForegroundColor Cyan
$cql2 = "SELECT taxi_id, speed, status FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id=$ZoneId LIMIT 200;"
$rows2 = Get-DataRows (Invoke-Cql $cql2)
$movN = 0; $movBad = 0; $idleN = 0; $idleBad = 0
foreach ($r in $rows2) {
    $cols = $r -split '\|' | ForEach-Object { $_.Trim() }
    if ($cols.Count -lt 3) { continue }
    $spd = [double]$cols[1]; $st = $cols[2]
    if ($st -eq "moving") {
        $movN++
        if ($spd -lt 1.0) { $movBad++ }
    } elseif ($st -in @("idle", "pickup", "dropoff")) {
        $idleN++
        # tolerant: pickup/dropoff can have transient non-zero speed
        if ($st -eq "idle" -and $spd -ge 5.0) { $idleBad++ }
    }
}
if ($movN -eq 0) {
    Write-Result "WARN" "No status=moving rows in sample."
} elseif ($movBad -eq 0) {
    Write-Result "PASS" "All $movN moving rows have speed >= 1 km/h."
} else {
    Write-Result "FAIL" "$movBad / $movN moving rows have speed < 1 km/h."
    $exit = 1
}
if ($idleN -gt 0) {
    if ($idleBad -eq 0) {
        Write-Result "PASS" "$idleN idle/pickup/dropoff rows have low/expected speed."
    } else {
        Write-Result "WARN" "$idleBad / $idleN idle rows have speed >= 5 km/h."
    }
}

# ── 3. snap_dist_m within threshold ─────────────────────────
Write-Host "`n[3] snap_dist_m <= $MaxSnapDistM m" -ForegroundColor Cyan
$cql3 = "SELECT taxi_id, snap_dist_m FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id=$ZoneId LIMIT 200;"
$rows3 = Get-DataRows (Invoke-Cql $cql3)
$snapN = 0; $snapBad = 0; $snapNull = 0
foreach ($r in $rows3) {
    $cols = $r -split '\|' | ForEach-Object { $_.Trim() }
    if ($cols.Count -lt 2) { continue }
    if ($cols[1] -eq "null" -or [string]::IsNullOrWhiteSpace($cols[1])) { $snapNull++; continue }
    $snapN++
    if ([double]$cols[1] -gt $MaxSnapDistM) { $snapBad++ }
}
if ($snapN -eq 0 -and $snapNull -gt 0) {
    Write-Result "WARN" "$snapNull rows have NULL snap_dist_m — schema migration / producer rebuild may not have propagated yet."
} elseif ($snapBad -eq 0) {
    Write-Result "PASS" "All $snapN rows with snap_dist_m are within $MaxSnapDistM m (NULL=$snapNull)."
} else {
    Write-Result "FAIL" "$snapBad / $snapN rows exceed $MaxSnapDistM m snap distance."
    $exit = 1
}

# ── 4. All latest points inside Casablanca bbox ─────────────
Write-Host "`n[4] Latest points inside Casablanca bbox" -ForegroundColor Cyan
$oob = 0; $bboxN = 0
foreach ($z in 1..16) {
    $cqlb = "SELECT lat, lon FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id=$z LIMIT 5;"
    $rowsb = Get-DataRows (Invoke-Cql $cqlb)
    foreach ($r in $rowsb) {
        $cols = $r -split '\|' | ForEach-Object { $_.Trim() }
        if ($cols.Count -lt 2) { continue }
        $lat = [double]$cols[0]; $lon = [double]$cols[1]
        $bboxN++
        if ($lat -lt $LAT_MIN -or $lat -gt $LAT_MAX -or $lon -lt $LON_MIN -or $lon -gt $LON_MAX) {
            $oob++
        }
    }
}
if ($bboxN -eq 0) {
    Write-Result "WARN" "No rows sampled across the 16 zones."
} elseif ($oob -eq 0) {
    Write-Result "PASS" "All $bboxN sampled latest points are inside the Casablanca bbox."
} else {
    Write-Result "FAIL" "$oob / $bboxN latest points are outside the Casablanca bbox."
    $exit = 1
}

Write-Host ""
if ($exit -eq 0) {
    Write-Host "=== ALL CHECKS PASSED ===" -ForegroundColor Green
} else {
    Write-Host "=== SOME CHECKS FAILED ===" -ForegroundColor Red
}
exit $exit
