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

    foreach ($path in @("build\web-portable", "dist\web-portable", "build\web-updater", "dist\web-updater")) {
        if (Test-Path $path) {
            Remove-Item -LiteralPath $path -Recurse -Force
        }
    }

    & $PythonExe -m PyInstaller --noconfirm --clean --workpath build\web-portable --distpath dist\web-portable apps\web\web_portable.spec
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    & $PythonExe -m PyInstaller --noconfirm --clean --workpath build\web-updater --distpath dist\web-updater apps\web\web_updater.spec
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    $outputDir = "dist\web-portable\bilipdj-web"
    $exe = "$outputDir\BiliPDJ-Web.exe"
    $updaterBuilt = "dist\web-updater\BiliPDJ-Web-Updater.exe"
    $updater = "$outputDir\BiliPDJ-Web-Updater.exe"
    if (-not (Test-Path $exe)) {
        throw "Build output missing: $exe"
    }
    if (-not (Test-Path $updaterBuilt)) {
        throw "Web updater output missing: $updaterBuilt"
    }
    Copy-Item -LiteralPath $updaterBuilt -Destination $updater -Force
    if (-not (Test-Path $updater)) {
        throw "Web portable bundle missing updater: $updater"
    }

    New-Item -ItemType Directory -Path "$outputDir\key" -Force | Out-Null
    @"
BiliPDJ 更新元数据目录

此目录用于保存更新检查 JSON 与 SHA-256 校验信息。Web Portable 支持全量更新、逐文件增量更新和本地版本恢复。
独立 BiliPDJ-Web-Updater.exe 会在主 Web 服务退出后继续提供更新进度 HTML，并负责备份、替换、失败回滚与重启。
请勿把 SHA-256 校验值当作加密私钥；正式签名私钥不会随发行包分发。
"@ | Set-Content -LiteralPath "$outputDir\key\README.txt" -Encoding utf8

    Write-Host "Web portable executable: $exe"
    Write-Host "Web portable updater: $updater"
    Write-Host "Update metadata directory: $outputDir\key"
}
finally {
    Pop-Location
}