# CloudFab Revit Connector - Build & Package ALL Versions (Single Installer)
#
# Usage:
#   .\build-installer-all.ps1                          # defaults to 2023,2024,2025,2026
#   .\build-installer-all.ps1 -Versions 2023,2025      # specific versions
#
# Output: dist\CloudFabRevit\  (zip this folder and send to recipient)

param(
  [string[]]$Versions = @("2023", "2024", "2025", "2026")
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

$versionList = $Versions -join ", "
Write-Host ""
Write-Host "  CloudFab Revit - Multi-Version Build & Package"
Write-Host "  ==============================================="
Write-Host "  Versions: $versionList"
Write-Host ""

# ── Paths ────────────────────────────────────────────────────────
$solutionDir   = Join-Path $root "ConnectorRevit"
$installerProj = Join-Path $root "Installer\CloudFabRevitInstaller.csproj"
$distDir       = Join-Path $root "dist\CloudFabRevit"

# Validate installer project exists
if (-not (Test-Path $installerProj)) {
  Write-Error "Installer project not found: $installerProj"
  exit 1
}

# Validate all version projects exist
foreach ($ver in $Versions) {
  $connProj = Join-Path $solutionDir "ConnectorRevit$ver\ConnectorRevit$ver.csproj"
  $convProj = Join-Path $root "Objects\Converters\ConverterRevit\ConverterRevit$ver\ConverterRevit$ver.csproj"
  foreach ($proj in @($connProj, $convProj)) {
    if (-not (Test-Path $proj)) {
      Write-Error "Project not found: $proj"
      exit 1
    }
  }
}

# ── Clean dist ───────────────────────────────────────────────────
if (Test-Path $distDir) {
  Remove-Item $distDir -Recurse -Force
}
New-Item $distDir -ItemType Directory | Out-Null

# ── Build each version ───────────────────────────────────────────
$totalVersions = $Versions.Count
$step = 0

foreach ($ver in $Versions) {
  $step++
  $connectorProj = Join-Path $solutionDir "ConnectorRevit$ver\ConnectorRevit$ver.csproj"
  $converterProj = Join-Path $root "Objects\Converters\ConverterRevit\ConverterRevit$ver\ConverterRevit$ver.csproj"
  $verDir        = Join-Path $distDir $ver

  New-Item $verDir -ItemType Directory | Out-Null

  # ── Build ConnectorRevit ─────────────────────────────────────
  Write-Host "  [$step/$totalVersions] Revit $ver - Building ConnectorRevit..."
  dotnet build $connectorProj -c Release "-p:SolutionDir=$solutionDir\" --nologo -v q
  if ($LASTEXITCODE -ne 0) { Write-Error "ConnectorRevit$ver build failed"; exit 1 }
  Write-Host "        Connector OK"

  # ── Build ConverterRevit ─────────────────────────────────────
  Write-Host "        Building ConverterRevit$ver..."
  dotnet build $converterProj -c Release -p:CopyToKitFolder=false --nologo -v q
  if ($LASTEXITCODE -ne 0) { Write-Error "ConverterRevit$ver build failed"; exit 1 }
  Write-Host "        Converter OK"

  # ── Gather Connector files ───────────────────────────────────
  $connectorDist = Join-Path $verDir "Connector"
  New-Item $connectorDist -ItemType Directory | Out-Null

  $releaseDir = Join-Path $solutionDir "Release\Release$ver"
  if (-not (Test-Path $releaseDir)) {
    $releaseDir = Join-Path $root "Release\Release$ver"
  }

  if (Test-Path $releaseDir) {
    $addinFile = Join-Path $releaseDir "CloudBridge.addin"
    Copy-Item $addinFile $connectorDist
    Copy-Item (Join-Path $releaseDir "CloudBridge") (Join-Path $connectorDist "CloudBridge") -Recurse
  } else {
    $binDir = Join-Path $solutionDir "ConnectorRevit$ver\bin\Release\win-x64"
    $addinFile = Get-ChildItem $binDir -Filter "CloudBridge.addin" -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty FullName
    if (-not $addinFile -or -not (Test-Path $addinFile)) {
      Write-Error "Could not find CloudBridge.addin for Revit $ver"
      exit 1
    }
    Copy-Item $addinFile $connectorDist
    $cloudBridgeDist = Join-Path $connectorDist "CloudBridge"
    New-Item $cloudBridgeDist -ItemType Directory | Out-Null
    Copy-Item "$binDir\*" $cloudBridgeDist -Recurse -Exclude "*.addin"
  }

  $connFileCount = (Get-ChildItem (Join-Path $connectorDist "CloudBridge") -Recurse -File).Count
  Write-Host "        Gathered $connFileCount connector files"

  # ── Gather Kit files ─────────────────────────────────────────
  $kitDist = Join-Path $verDir "Kit"
  New-Item $kitDist -ItemType Directory | Out-Null

  # Objects.dll
  $objectsBinDir = Join-Path $root "Objects\Objects\bin\Release"
  $objectsDll = Get-ChildItem $objectsBinDir -Recurse -Filter "Objects.dll" -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($objectsDll) {
    Copy-Item $objectsDll.FullName $kitDist
  } else {
    Write-Warning "Objects.dll not found - may not have been built as a dependency"
  }

  # Version-specific converter DLL
  $converterBinDir = Join-Path $root "Objects\Converters\ConverterRevit\ConverterRevit$ver\bin\Release"
  $converterDll = Get-ChildItem $converterBinDir -Recurse -Filter "Objects.Converter.Revit$ver.dll" -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($converterDll) {
    Copy-Item $converterDll.FullName $kitDist
  } else {
    Write-Warning "Objects.Converter.Revit$ver.dll not found"
  }

  # Templates
  $templatesSrc = Join-Path $root "Objects\Converters\ConverterRevit\Templates\$ver"
  if (Test-Path $templatesSrc) {
    $templatesDist = Join-Path $kitDist "Templates\Revit\$ver"
    New-Item $templatesDist -ItemType Directory -Force | Out-Null
    Copy-Item "$templatesSrc\*" $templatesDist
    $templateCount = (Get-ChildItem $templatesDist -File).Count
    Write-Host "        Gathered $templateCount template files"
  }

  Write-Host ""
}

# ── Publish installer exe ──────────────────────────────────────
Write-Host "  Publishing installer exe..."
$publishDir = Join-Path $distDir "__publish"
dotnet publish $installerProj -c Release -o $publishDir --nologo -v q
if ($LASTEXITCODE -ne 0) { Write-Error "Installer publish failed"; exit 1 }

Copy-Item (Join-Path $publishDir "CloudFabInstaller.exe") $distDir
Remove-Item $publishDir -Recurse -Force
Write-Host "  OK"

# ── Summary ──────────────────────────────────────────────────────
Write-Host ""
Write-Host "  Done! Distribution package:"
Write-Host "  $distDir"
Write-Host ""
Write-Host "  Contents:"
Write-Host '    CloudFabRevitInstaller.exe   <- recipient runs this'
foreach ($ver in $Versions) {
  Write-Host ('    ' + $ver + '\')
  Write-Host '      Connector\  (CloudBridge.addin + DLLs)'
  Write-Host '      Kit\        (Objects DLLs + templates)'
}
Write-Host ''
Write-Host '  Zip this folder and send it. Recipient runs the exe.'
Write-Host ''