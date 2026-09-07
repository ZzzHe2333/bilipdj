param(
    [switch]$InstallDependencies,
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
& "$PSScriptRoot\apps\windows\package.ps1" -InstallDependencies:$InstallDependencies -PythonExe $PythonExe
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
