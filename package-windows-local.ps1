param(
    [switch]$InstallDependencies,
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $scriptDir

try {
    foreach ($shadowPath in @("http.py", "http\\__init__.py")) {
        if (Test-Path $shadowPath) {
            throw "Found shadowing file: $shadowPath"
        }
    }

    if ($InstallDependencies) {
        & $PythonExe -m pip install --upgrade pip
        if (Test-Path requirements.txt) {
            & $PythonExe -m pip install -r requirements.txt
        }
        & $PythonExe -m pip uninstall -y http 2>$null
        Write-Host "http uninstall done (or not installed)"
        & $PythonExe -m pip install pyinstaller 'qrcode[pil]' brotli psutil
    }

    foreach ($buildPath in @("build", "dist")) {
        if (Test-Path $buildPath) {
            Remove-Item -LiteralPath $buildPath -Recurse -Force
        }
    }

    & $PythonExe -m PyInstaller --noconfirm --clean bilipdj_onedir.spec
    & $PythonExe -m PyInstaller --noconfirm --clean paiduijitm.spec

    if (Test-Path "dist\\bilipdj\\core\\cd") {
        Remove-Item -LiteralPath "dist\\bilipdj\\core\\cd" -Recurse -Force
    }

    if (-not (Test-Path "dist\\bilipdj\\main.exe")) {
        throw "Build output missing: dist\\bilipdj\\main.exe"
    }

    if (-not (Test-Path "dist\\paiduijitm.exe")) {
        throw "Build output missing: dist\\paiduijitm.exe"
    }

    Copy-Item "dist\\paiduijitm.exe" "dist\\bilipdj\\paiduijitm.exe" -Force

    Write-Host "Main panel executable: dist\\bilipdj\\main.exe"
    Write-Host "Overlay executable: dist\\bilipdj\\paiduijitm.exe"
}
finally {
    Pop-Location
}
