param(
    [switch]$InstallDependencies,
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Push-Location $repoRoot

try {
    if ($InstallDependencies) {
        & $PythonExe -m pip install --upgrade pip
        & $PythonExe -m pip install -r requirements.txt
        & $PythonExe -m pip install pyinstaller
    }

    & $PythonExe apps\web\build.py
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    foreach ($path in @("build\web-portable", "dist\web-portable")) {
        if (Test-Path $path) {
            Remove-Item -LiteralPath $path -Recurse -Force
        }
    }

    & $PythonExe -m PyInstaller --noconfirm --clean --workpath build\web-portable --distpath dist\web-portable apps\web\web_portable.spec
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    $exe = "dist\web-portable\bilipdj-web\BiliPDJ-Web.exe"
    if (-not (Test-Path $exe)) {
        throw "Build output missing: $exe"
    }

    Write-Host "Web portable executable: $exe"
}
finally {
    Pop-Location
}
