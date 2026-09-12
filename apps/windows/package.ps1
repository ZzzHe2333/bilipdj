param(
    [switch]$InstallDependencies,
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Invoke-FrozenProbe {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string]$Arguments,
        [Parameter(Mandatory = $true)][string]$Label,
        [int]$TimeoutSeconds = 30
    )

    $process = Start-Process -FilePath $FilePath -ArgumentList $Arguments -PassThru
    $deadline = (Get-Date).AddSeconds([Math]::Max(5, $TimeoutSeconds))
    while (-not $process.HasExited -and (Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 200
        $process.Refresh()
    }
    if (-not $process.HasExited) {
        try { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue } catch {}
        throw "$Label timed out after $TimeoutSeconds seconds"
    }
    return [int]$process.ExitCode
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repoRoot

try {
    foreach ($shadowPath in @("http.py", "http\__init__.py")) {
        if (Test-Path $shadowPath) {
            throw "Found shadowing file: $shadowPath"
        }
    }

    if ($InstallDependencies) {
        & $PythonExe -m pip install --upgrade pip
        & $PythonExe -m pip install -r requirements.txt
        & $PythonExe -m pip uninstall -y http 2>$null
        Write-Host "http uninstall done (or not installed)"
        & $PythonExe -m pip install pyinstaller
    }

    foreach ($buildPath in @("build", "dist")) {
        if (Test-Path $buildPath) {
            Remove-Item -LiteralPath $buildPath -Recurse -Force
        }
    }

    & $PythonExe apps\web\build.py
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    & $PythonExe -m PyInstaller --noconfirm --clean apps\windows\bilipdj_onedir.spec
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $PythonExe -m PyInstaller --noconfirm --clean apps\windows\paiduijitm.spec
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $PythonExe -m PyInstaller --noconfirm --clean apps\windows\updater.spec
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    if (Test-Path "dist\bilipdj\core\cd") {
        Remove-Item -LiteralPath "dist\bilipdj\core\cd" -Recurse -Force
    }

    foreach ($requiredPath in @("dist\bilipdj\main.exe", "dist\paiduijitm.exe", "dist\updater.exe")) {
        if (-not (Test-Path $requiredPath)) {
            throw "Build output missing: $requiredPath"
        }
    }

    Copy-Item "dist\paiduijitm.exe" "dist\bilipdj\paiduijitm.exe" -Force
    Copy-Item "dist\updater.exe" "dist\bilipdj\updater.exe" -Force

    # A successful PyInstaller build is not enough: exercise the exact CTk
    # startup path, but never let a frozen child/background resource hang CI.
    $mainExe = (Resolve-Path "dist\bilipdj\main.exe").Path
    $startupExit = Invoke-FrozenProbe -FilePath $mainExe -Arguments "--gui-startup-self-test" -Label "Frozen Windows GUI startup self-test" -TimeoutSeconds 30
    if ($startupExit -ne 0) {
        $startupLog = "dist\bilipdj\log\gui-startup-error.log"
        Write-Host "===== BiliPDJ GUI startup diagnostics ====="
        if (Test-Path $startupLog) {
            Get-Content -LiteralPath $startupLog -Raw | Write-Host
        }
        else {
            Write-Host "No GUI startup log was produced."
        }
        Write-Host "===== end diagnostics ====="
        throw "Frozen Windows GUI startup self-test failed with exit code $startupExit"
    }

    New-Item -ItemType Directory -Path "dist\bilipdj\key" -Force | Out-Null
    @"
BiliPDJ 更新元数据目录

此目录由内置更新器使用，用于保存更新检查 JSON、逐文件清单缓存、SHA-256 校验值和 update-result.json。
请勿把 SHA-256 校验值当作加密私钥；正式签名私钥不会随发行包分发。
"@ | Set-Content -LiteralPath "dist\bilipdj\key\README.txt" -Encoding utf8

    Write-Host "Main panel executable: dist\bilipdj\main.exe"
    Write-Host "Overlay executable: dist\bilipdj\paiduijitm.exe"
    Write-Host "Updater executable: dist\bilipdj\updater.exe"
    Write-Host "Update metadata directory: dist\bilipdj\key"
}
finally {
    Pop-Location
}
