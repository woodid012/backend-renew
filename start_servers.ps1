# Start Backend and Frontend Servers
Write-Host "Starting servers..." -ForegroundColor Green

# Start Backend
Write-Host "`n[1/2] Starting Backend on port 10000..." -ForegroundColor Yellow
$backend = Start-Process python -ArgumentList "app.py" -WorkingDirectory "c:\Projects\backend-renew" -PassThru -WindowStyle Normal
Write-Host "Backend process started with PID: $($backend.Id)" -ForegroundColor Cyan

# Start Frontend  
Write-Host "`n[2/2] Starting Frontend on port 3000..." -ForegroundColor Yellow
$frontend = Start-Process npm -ArgumentList "run", "dev" -WorkingDirectory "c:\Projects\renew-front" -PassThru -WindowStyle Normal
Write-Host "Frontend process started with PID: $($frontend.Id)" -ForegroundColor Cyan

# Wait a bit for servers to start
Write-Host "`nWaiting 15 seconds for servers to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# Check if servers are responding
Write-Host "`nChecking server status..." -ForegroundColor Yellow

# Check Backend
try {
    $backendResponse = Invoke-WebRequest -Uri "http://localhost:10000" -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
    Write-Host "✅ Backend is RUNNING on http://localhost:10000 (Status: $($backendResponse.StatusCode))" -ForegroundColor Green
} catch {
    Write-Host "❌ Backend is NOT responding: $($_.Exception.Message)" -ForegroundColor Red
}

# Check Frontend
try {
    $frontendResponse = Invoke-WebRequest -Uri "http://localhost:3000" -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
    Write-Host "✅ Frontend is RUNNING on http://localhost:3000 (Status: $($frontendResponse.StatusCode))" -ForegroundColor Green
} catch {
    Write-Host "❌ Frontend is NOT responding: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`nServers are running in separate windows. Close those windows to stop the servers." -ForegroundColor Cyan
Write-Host "Press any key to exit this script (servers will continue running)..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

