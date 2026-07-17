param([switch]$InstallDependencies, [string]$PythonExe = "python")
$ErrorActionPreference = "Stop"
$arch = (& $PythonExe -c "import platform; print(platform.machine().lower())").Trim()
if ($arch -notin @("amd64", "x86_64")) { throw "amd64 build requires an amd64 runner; detected: $arch" }
& "$PSScriptRoot\..\package-windows-local.ps1" -InstallDependencies:$InstallDependencies -PythonExe $PythonExe
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
