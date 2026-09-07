param(
    [switch]$InstallDependencies,
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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

    Write-Host "Main panel executable: dist\bilipdj\main.exe"
    Write-Host "Overlay executable: dist\bilipdj\paiduijitm.exe"
    Write-Host "Updater executable: dist\bilipdj\updater.exe"
}
finally {
    Pop-Location
}
