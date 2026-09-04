@echo off
title SENTINEL-RTO Risk Engine
color 0A
echo.
echo  ====================================
echo   SENTINEL-RTO Risk Engine
echo   Starting up...
echo  ====================================
echo.
echo  [1/2] Starting the risk engine server...
echo.

cd /d "%~dp0"

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    color 0C
    echo  ERROR: Python is not installed.
    echo  Please install Python from https://python.org
    echo.
    pause
    exit /b 1
)

REM Check if dependencies are installed
python -c "import fastapi" >nul 2>&1
if errorlevel 1 (
    echo  Installing dependencies (first time only)...
    python -m pip install fastapi uvicorn pydantic pydantic-settings httpx redis aiokafka lightgbm numpy pandas scikit-learn onnxruntime onnxmltools onnx skl2onnx sqlalchemy alembic asyncpg --quiet
    echo  Dependencies installed.
    echo.
)

REM Check if model exists, if not generate data and train
if not exist "models\sentinel_lgbm.onnx" (
    echo  [*] First run: Generating training data...
    python -m src.ml.synthetic_data
    echo  [*] Training ML model...
    python -m src.ml.train_model
    echo  [*] Model ready.
    echo.
)

echo  [2/2] Opening dashboard in your browser...
echo.

REM Open browser after a short delay
start "" cmd /c "timeout /t 3 /nobreak >nul && start http://localhost:8000"

REM Start the server (this blocks)
echo  Server running at http://localhost:8000
echo  Press Ctrl+C to stop.
echo.
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000

pause
