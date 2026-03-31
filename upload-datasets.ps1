# ============================================================
# TaaSim — Task 2: Download & Upload Datasets to MinIO
# Run AFTER docker-compose is up and MinIO is healthy
# ============================================================

$ErrorActionPreference = "Stop"
$BASE_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$DATA_DIR = Join-Path $BASE_DIR "data"

New-Item -ItemType Directory -Force -Path $DATA_DIR | Out-Null

# ──────────────────────────────────────────────
# STEP 1: Download Porto Taxi Trajectories
# ──────────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 1: Porto Taxi Trajectories ===" -ForegroundColor Cyan

$PORTO_FILE = Join-Path $DATA_DIR "train.csv"

if (Test-Path $PORTO_FILE) {
    Write-Host "Porto dataset already exists — skipping download." -ForegroundColor Green
} else {
    $kaggleExists = Get-Command kaggle -ErrorAction SilentlyContinue
    if ($kaggleExists) {
        Write-Host "Kaggle CLI found! Downloading..." -ForegroundColor Green
        try {
            kaggle competitions download -c pkdd-15-predict-taxi-service-trajectory-i -p $DATA_DIR
            $zipFile = Get-ChildItem -Path $DATA_DIR -Filter "*.zip" | Select-Object -First 1
            if ($zipFile) {
                Write-Host "Extracting $($zipFile.Name)..."
                Expand-Archive -Path $zipFile.FullName -DestinationPath $DATA_DIR -Force
                Remove-Item $zipFile.FullName
                Write-Host "Porto dataset extracted!" -ForegroundColor Green
            }
        } catch {
            Write-Host "Kaggle download failed: $_" -ForegroundColor Red
            Write-Host "Please download manually." -ForegroundColor Yellow
        }
    } else {
        Write-Host "Kaggle CLI not found. Download manually:" -ForegroundColor Yellow
        Write-Host "  OPTION A (CLI):" -ForegroundColor White
        Write-Host "    pip install kaggle" -ForegroundColor Gray
        Write-Host "    Get token: https://www.kaggle.com/settings -> Create New Token" -ForegroundColor Gray
        Write-Host "    Save kaggle.json to: C:\Users\$env:USERNAME\.kaggle\" -ForegroundColor Gray
        Write-Host "    kaggle competitions download -c pkdd-15-predict-taxi-service-trajectory-i -p `"$DATA_DIR`"" -ForegroundColor Gray
        Write-Host ""
        Write-Host "  OPTION B (Browser):" -ForegroundColor White
        Write-Host "    https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i/data" -ForegroundColor Gray
        Write-Host "    Download train.csv.zip, extract train.csv to: $DATA_DIR" -ForegroundColor Gray
        Write-Host ""
        Write-Host "Press Enter after placing train.csv in $DATA_DIR ..."
        Read-Host
    }
}

if (-not (Test-Path $PORTO_FILE)) {
    Write-Host "ERROR: train.csv not found at $PORTO_FILE" -ForegroundColor Red
    exit 1
}
$portoSize = [math]::Round((Get-Item $PORTO_FILE).Length / 1MB, 1)
Write-Host "Porto ready: train.csv ($portoSize MB)" -ForegroundColor Green

# ──────────────────────────────────────────────
# STEP 2: Download NYC TLC Trip Records (3 months)
# ──────────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 2: NYC TLC Trip Records ===" -ForegroundColor Cyan

$NYC_DIR = Join-Path $DATA_DIR "nyc-tlc"
New-Item -ItemType Directory -Force -Path $NYC_DIR | Out-Null

$months = @(
    @{ url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"; file = "yellow_tripdata_2023-01.parquet" },
    @{ url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet"; file = "yellow_tripdata_2023-02.parquet" },
    @{ url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet"; file = "yellow_tripdata_2023-03.parquet" }
)

foreach ($m in $months) {
    $filePath = Join-Path $NYC_DIR $m.file
    if (Test-Path $filePath) {
        Write-Host "$($m.file) already exists — skipping." -ForegroundColor Green
    } else {
        Write-Host "Downloading $($m.file)..."
        try {
            Invoke-WebRequest -Uri $m.url -OutFile $filePath -UseBasicParsing
            $fileSize = [math]::Round((Get-Item $filePath).Length / 1MB, 1)
            Write-Host "  Done: $fileSize MB" -ForegroundColor Green
        } catch {
            Write-Host "  FAILED: $_" -ForegroundColor Red
            Write-Host "  Manual URL: $($m.url)" -ForegroundColor Yellow
        }
    }
}

Write-Host ""
Write-Host "NYC TLC files:" -ForegroundColor Green
Get-ChildItem $NYC_DIR -Filter "*.parquet" | ForEach-Object {
    Write-Host "  $($_.Name) — $([math]::Round($_.Length / 1MB, 1)) MB"
}

# ──────────────────────────────────────────────
# STEP 3: Upload to MinIO
# ──────────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 3: Upload datasets to MinIO ===" -ForegroundColor Cyan

# Detect Docker network name
$networkName = (docker network ls --format "{{.Name}}" | Where-Object { $_ -like "*taasim*" } | Select-Object -First 1)
if (-not $networkName) { $networkName = "taasimm_taasim-net" }
Write-Host "Using Docker network: $networkName" -ForegroundColor Gray

# Upload Porto
Write-Host "Uploading Porto dataset to MinIO..."
docker run --rm --network $networkName `
    -v "${DATA_DIR}:/data" `
    --entrypoint "" `
    minio/mc sh -c "mc alias set myminio http://minio:9000 minioadmin minioadmin && mc cp /data/train.csv myminio/raw/porto-trips/train.csv"

# Upload NYC TLC
Write-Host "Uploading NYC TLC Parquet files to MinIO..."
docker run --rm --network $networkName `
    -v "${NYC_DIR}:/data/nyc" `
    --entrypoint "" `
    minio/mc sh -c "mc alias set myminio http://minio:9000 minioadmin minioadmin && mc cp --recursive /data/nyc/ myminio/raw/nyc-tlc/"

# ──────────────────────────────────────────────
# STEP 4: Verify uploads
# ──────────────────────────────────────────────
Write-Host ""
Write-Host "=== STEP 4: Verifying uploads ===" -ForegroundColor Cyan

docker run --rm --network $networkName `
    --entrypoint "" `
    minio/mc sh -c "mc alias set myminio http://minio:9000 minioadmin minioadmin; echo '=== All Buckets ==='; mc ls myminio; echo ''; echo '=== Porto Trips ==='; mc ls myminio/raw/porto-trips/; echo ''; echo '=== NYC TLC ==='; mc ls myminio/raw/nyc-tlc/"

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host " Task 2 Complete: Datasets uploaded to MinIO" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host "Verify via MinIO Console: http://localhost:9001" -ForegroundColor Cyan
Write-Host "Login: minioadmin / minioadmin" -ForegroundColor Cyan
