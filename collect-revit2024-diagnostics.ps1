#requires -Version 3
# CloudBridge for Revit 2024 - diagnostic collector
# WHAT IT DOES:
#   1. Turns on .NET assembly bind logging (so we can see which DLL fails to load)
#   2. Waits while you reproduce the "cannot run the external application" error in Revit 2024
#   3. Collects the Revit journal, Fusion bind logs, Speckle logs, installed DLL
#      versions and recent .NET Runtime crash events into a single .zip on your Desktop
#   4. Turns the bind logging back off
#
# HOW TO RUN:
#   Right-click the Start button > "Windows PowerShell (Admin)" / "Terminal (Admin)",
#   then paste this line (adjust the path to where you saved this file):
#       powershell -ExecutionPolicy Bypass -File "$HOME\Downloads\collect-revit2024-diagnostics.ps1"

$ErrorActionPreference = 'Continue'

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)
if (-not $isAdmin) {
  Write-Host "ERROR: Please re-run this in an ELEVATED PowerShell (Run as administrator)." -ForegroundColor Red
  Read-Host "Press Enter to exit"
  exit 1
}

$fusionDir = 'C:\FusionLog'
$fusionKey = 'HKLM:\SOFTWARE\Microsoft\Fusion'

Write-Host "Enabling .NET assembly bind logging..." -ForegroundColor Cyan
New-Item -ItemType Directory -Path $fusionDir -Force | Out-Null
New-Item -Path $fusionKey -Force | Out-Null
Set-ItemProperty -Path $fusionKey -Name 'EnableLog'   -Type DWord  -Value 1
Set-ItemProperty -Path $fusionKey -Name 'LogFailures' -Type DWord  -Value 1
Set-ItemProperty -Path $fusionKey -Name 'ForceLog'    -Type DWord  -Value 1
Set-ItemProperty -Path $fusionKey -Name 'LogPath'     -Type String -Value ($fusionDir + '\')

Write-Host ""
Write-Host "=== DO THIS NOW ===" -ForegroundColor Yellow
Write-Host "1. Fully close Revit 2024 if it is open."
Write-Host "2. Start Revit 2024 and wait for the error dialog(s) to appear."
Write-Host "3. Note every popup and the ORDER it appeared (write it down)."
Write-Host "4. On the 'cannot run the external application' dialog, click 'Show details'"
Write-Host "   and take a screenshot."
Write-Host "5. Close Revit 2024 completely."
Write-Host ""
Read-Host "When you have reproduced the error and CLOSED Revit 2024, press Enter to collect logs"

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$out   = Join-Path ([Environment]::GetFolderPath('Desktop')) ("CloudBridge_Revit2024_Diag_" + $stamp)
New-Item -ItemType Directory -Path $out -Force | Out-Null

Write-Host "Collecting..." -ForegroundColor Cyan

# Fusion assembly bind logs
if (Test-Path $fusionDir) {
  Copy-Item $fusionDir -Destination (Join-Path $out 'FusionLog') -Recurse -Force -ErrorAction SilentlyContinue
}

# Revit 2024 journals (newest 3)
$jdir = Join-Path $env:LOCALAPPDATA 'Autodesk\Revit\Autodesk Revit 2024\Journals'
if (Test-Path $jdir) {
  $jout = Join-Path $out 'Journals'
  New-Item -ItemType Directory -Path $jout -Force | Out-Null
  Get-ChildItem $jdir -Filter 'journal*.txt' | Sort-Object LastWriteTime -Descending |
    Select-Object -First 3 | ForEach-Object { Copy-Item $_.FullName $jout -Force }
}

# Speckle logs (recent files + full folder listing so we can see if a Revit 2024 folder was ever created)
$slog = Join-Path $env:APPDATA 'Speckle\Logs'
if (Test-Path $slog) {
  $sout = Join-Path $out 'SpeckleLogs'
  New-Item -ItemType Directory -Path $sout -Force | Out-Null
  Get-ChildItem $slog -Recurse -Filter '*.txt' -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -gt (Get-Date).AddDays(-2) } |
    ForEach-Object { Copy-Item $_.FullName $sout -Force -ErrorAction SilentlyContinue }
  Get-ChildItem $slog -Directory -ErrorAction SilentlyContinue |
    Select-Object Name, LastWriteTime | Out-File (Join-Path $out 'SpeckleLogs_folders.txt')
}

# Installed addin manifests + DLL versions
$addinDir = Join-Path $env:APPDATA 'Autodesk\Revit\Addins\2024'
if (Test-Path $addinDir) {
  Get-ChildItem $addinDir -Filter '*.addin' -ErrorAction SilentlyContinue |
    ForEach-Object { Copy-Item $_.FullName (Join-Path $out ('addin_' + $_.Name)) -Force }
  $cbFolder = Join-Path $addinDir 'CloudBridge'
  if (Test-Path $cbFolder) {
    $cb = Join-Path $cbFolder 'CloudBridgeConnectorRevit.dll'
    if (Test-Path $cb) {
      $ver = try { [System.Reflection.AssemblyName]::GetAssemblyName($cb).Version.ToString() } catch { 'n/a' }
      "CloudBridgeConnectorRevit.dll  AssemblyVersion=$ver  LastWrite=$((Get-Item $cb).LastWriteTime)" |
        Out-File (Join-Path $out 'CloudBridge_version.txt')
    }
    Get-ChildItem $cbFolder -Filter '*.dll' -ErrorAction SilentlyContinue | ForEach-Object {
      $v = try { [System.Reflection.AssemblyName]::GetAssemblyName($_.FullName).Version.ToString() } catch { 'n/a' }
      ("{0}`t{1}`t{2}" -f $_.Name, $v, $_.LastWriteTime)
    } | Out-File (Join-Path $out 'addin_dll_versions.txt')
  }
}

# Revit 2024 install version + its binding-redirect config
$revitExe = 'C:\Program Files\Autodesk\Revit 2024\Revit.exe'
if (Test-Path $revitExe) {
  (Get-Item $revitExe).VersionInfo | Format-List * | Out-File (Join-Path $out 'Revit_version.txt')
}
$revitCfg = 'C:\Program Files\Autodesk\Revit 2024\Revit.exe.config'
if (Test-Path $revitCfg) { Copy-Item $revitCfg (Join-Path $out 'Revit.exe.config') -Force }

# Versions of the conflict-prone assemblies that Revit 2024 itself ships (for comparison)
$revitDir = 'C:\Program Files\Autodesk\Revit 2024'
if (Test-Path $revitDir) {
  $watch = 'System.Runtime.CompilerServices.Unsafe','System.Text.Json','System.Memory','System.Buffers','Microsoft.Bcl.AsyncInterfaces','System.Text.Encodings.Web','System.Collections.Immutable','System.Threading.Tasks.Extensions','Newtonsoft.Json'
  $rows = foreach ($name in $watch) {
    Get-ChildItem $revitDir -Recurse -Filter ($name + '.dll') -ErrorAction SilentlyContinue | ForEach-Object {
      $v = try { [System.Reflection.AssemblyName]::GetAssemblyName($_.FullName).Version.ToString() } catch { 'n/a' }
      ("{0}`t{1}`t{2}" -f $v, $_.FullName, $_.LastWriteTime)
    }
  }
  $rows | Out-File (Join-Path $out 'revit2024_shipped_assembly_versions.txt')
}

# Recent .NET Runtime / Application Error crash events
try {
  Get-WinEvent -FilterHashtable @{ LogName='Application'; StartTime=(Get-Date).AddHours(-1) } -ErrorAction SilentlyContinue |
    Where-Object { $_.ProviderName -match 'NET Runtime|Application Error' } |
    Select-Object TimeCreated, ProviderName, Id, Message | Format-List * |
    Out-File (Join-Path $out 'DotNetRuntime_events.txt')
} catch {}

# Turn bind logging back off (it is global and slows the machine)
Write-Host "Disabling .NET assembly bind logging..." -ForegroundColor Cyan
Set-ItemProperty -Path $fusionKey -Name 'EnableLog'   -Type DWord -Value 0
Set-ItemProperty -Path $fusionKey -Name 'ForceLog'    -Type DWord -Value 0
Set-ItemProperty -Path $fusionKey -Name 'LogFailures' -Type DWord -Value 0

$zip = $out + '.zip'
if (Test-Path $zip) { Remove-Item $zip -Force }
Compress-Archive -Path $out -DestinationPath $zip -Force

Write-Host ""
Write-Host ("DONE. Please send this file back:") -ForegroundColor Green
Write-Host ("    " + $zip) -ForegroundColor Green
Read-Host "Press Enter to exit"
