#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Ensure the live Cassandra schema matches the current TaaSim pipeline contract.

.DESCRIPTION
    Handles schema drift caused by persisted Cassandra volumes that predate
    newer columns required by the Flink jobs.
#>

$ErrorActionPreference = "Stop"

Write-Host "=== Ensuring Cassandra Schema ===" -ForegroundColor Cyan

function Get-TableDefinition {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TableName
    )

    docker exec taasim-cassandra cqlsh -e "USE taasim; DESCRIBE TABLE $TableName;" 2>$null
}

function Ensure-Column {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TableName,

        [Parameter(Mandatory = $true)]
        [string]$ColumnName,

        [Parameter(Mandatory = $true)]
        [string]$ColumnType
    )

    $tableDef = Get-TableDefinition -TableName $TableName
    if (-not $tableDef) {
        throw "Could not describe table '$TableName'. Is Cassandra healthy and keyspace initialized?"
    }

    if ($tableDef -match "(?im)^\s*$ColumnName\s+$ColumnType\b") {
        Write-Host "  OK: $TableName.$ColumnName exists" -ForegroundColor Green
        return
    }

    Write-Host "  ALTER: adding $TableName.$ColumnName $ColumnType" -ForegroundColor Yellow
    docker exec taasim-cassandra cqlsh -e "USE taasim; ALTER TABLE $TableName ADD $ColumnName $ColumnType;" | Out-Null
    Write-Host "  DONE: added $TableName.$ColumnName" -ForegroundColor Green
}

Ensure-Column -TableName "vehicle_positions" -ColumnName "h3_index" -ColumnType "text"
Ensure-Column -TableName "trips" -ColumnName "origin_h3" -ColumnType "text"
Ensure-Column -TableName "trips" -ColumnName "dest_h3" -ColumnType "text"

Write-Host "=== Cassandra Schema Ready ===" -ForegroundColor Green