# CloudFab Revit Connector - Build & Package Installer
#
# Usage:
#   .\build-installer.ps1                    # defaults to Revit 2025
#   .\build-installer.ps1 -RevitVersion 2026
#
# Output: dist\CloudFabRevit{version}\  (zip this folder and send to recipient)

param(
  [string]$RevitVersion = "2025"
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

Write-Host ""
Write-Host "  CloudFab Revit $RevitVersion - Build & Package"
Write-Host "  =============================================="
Write-Host ""

# ── Paths ────────────────────────────────────────────────────────
$solutionDir   = Join-Path $root "ConnectorRevit"
$connectorProj = Join-Path $solutionDir "ConnectorRevit$RevitVersion\ConnectorRevit$RevitVersion.csproj"
$converterProj = Join-Path $root "Objects\Converters\ConverterRevit\ConverterRevit$RevitVersion\ConverterRevit$RevitVersion.csproj"
$installerProj = Join-Path $root "Installer\CloudFabRevitInstaller.csproj"
$distDir       = Join-Path $root "dist\CloudFabRevit$RevitVersion"

# Validate projects exist
foreach ($proj in @($connectorProj, $converterProj, $installerProj)) {
  if (-not (Test-Path $proj)) {
    Write-Error "Project not found: $proj"
    exit 1
  }
}

# ── Clean dist ───────────────────────────────────────────────────
if (Test-Path $distDir) {
  Remove-Item $distDir -Recurse -Force
}
New-Item $distDir -ItemType Directory | Out-Null

# ── Step 1: Build ConnectorRevit (Release) ───────────────────────
Write-Host "  [1/4] Building ConnectorRevit$RevitVersion (Release)..."
# Pass SolutionDir so AfterBuildRelease target knows where to stage
dotnet build $connectorProj -c Release "-p:SolutionDir=$solutionDir\" --nologo -v q
if ($LASTEXITCODE -ne 0) { Write-Error "ConnectorRevit build failed"; exit 1 }
Write-Host "        OK"

# ── Step 2: Build ConverterRevit (Release) ───────────────────────
Write-Host "  [2/4] Building ConverterRevit$RevitVersion (Release)..."
dotnet build $converterProj -c Release --nologo -v q
if ($LASTEXITCODE -ne 0) { Write-Error "ConverterRevit build failed"; exit 1 }
Write-Host "        OK"

# ── Step 3: Gather files into dist ───────────────────────────────
Write-Host "  [3/4] Gathering files..."

# --- Connector files ---
$connectorDist = Join-Path $distDir "Connector"
New-Item $connectorDist -ItemType Directory | Out-Null

# Try Release staging dir first, fall back to bin output
$releaseDir = Join-Path $solutionDir "Release\Release$RevitVersion"
if (-not (Test-Path $releaseDir)) {
  $releaseDir = Join-Path $root "Release\Release$RevitVersion"
}

if (Test-Path $releaseDir) {
  # Use the Release staging output
  $addinFile = Join-Path $releaseDir "CloudBridge.addin"
  $dllFolder = Join-Path $releaseDir "CloudBridge"
} else {
  # Fall back to build output directory
  $binDir = Join-Path $solutionDir "ConnectorRevit$RevitVersion\bin\Release\win-x64"
  $addinFile = Get-ChildItem $binDir -Filter "CloudBridge.addin" -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty FullName
  $dllFolder = $binDir
}

if (-not $addinFile -or -not (Test-Path $addinFile)) {
  Write-Error "Could not find CloudBridge.addin in release output"
  exit 1
}

Copy-Item $addinFile $connectorDist

if (Test-Path (Join-Path $releaseDir "CloudBridge")) {
  Copy-Item (Join-Path $releaseDir "CloudBridge") (Join-Path $connectorDist "CloudBridge") -Recurse
} else {
  # bin output doesn't have CloudBridge subfolder — the DLLs ARE the output
  $cloudBridgeDist = Join-Path $connectorDist "CloudBridge"
  New-Item $cloudBridgeDist -ItemType Directory | Out-Null
  Copy-Item "$dllFolder\*" $cloudBridgeDist -Recurse -Exclude "*.addin"
}

$connectorFileCount = (Get-ChildItem (Join-Path $connectorDist "CloudBridge") -Recurse -File).Count
Write-Host "        Connector: $connectorFileCount files"

# --- Kit files (Objects DLLs + templates) ---
$kitDist = Join-Path $distDir "Kit"
New-Item $kitDist -ItemType Directory | Out-Null

# Find Objects.dll from the Objects project build output
$objectsBinDir = Join-Path $root "Objects\Objects\bin\Release"
$objectsDll = Get-ChildItem $objectsBinDir -Recurse -Filter "Objects.dll" -ErrorAction SilentlyContinue | Select-Object -First 1
if ($objectsDll) {
  Copy-Item $objectsDll.FullName $kitDist
  Write-Host "        Kit: Objects.dll"
} else {
  Write-Warning "Objects.dll not found in build output — it may not have been built as a dependency"
}

# Find Objects.Converter.RevitXXXX.dll
$converterBinDir = Join-Path $root "Objects\Converters\ConverterRevit\ConverterRevit$RevitVersion\bin\Release"
$converterDll = Get-ChildItem $converterBinDir -Recurse -Filter "Objects.Converter.Revit$RevitVersion.dll" -ErrorAction SilentlyContinue | Select-Object -First 1
if ($converterDll) {
  Copy-Item $converterDll.FullName $kitDist
  Write-Host "        Kit: Objects.Converter.Revit$RevitVersion.dll"
} else {
  Write-Warning "Objects.Converter.Revit$RevitVersion.dll not found"
}

# Templates
$templatesSrc = Join-Path $root "Objects\Converters\ConverterRevit\Templates\$RevitVersion"
if (Test-Path $templatesSrc) {
  $templatesDist = Join-Path $kitDist "Templates\Revit\$RevitVersion"
  New-Item $templatesDist -ItemType Directory -Force | Out-Null
  Copy-Item "$templatesSrc\*" $templatesDist
  $templateCount = (Get-ChildItem $templatesDist -File).Count
  Write-Host "        Kit: $templateCount template files"
}

# ── Step 4: Publish installer exe ────────────────────────────────
Write-Host "  [4/4] Publishing installer exe..."
$publishDir = Join-Path $distDir "__publish"
dotnet publish $installerProj -c Release -o $publishDir "-p:RevitVersion=$RevitVersion" --nologo -v q
if ($LASTEXITCODE -ne 0) { Write-Error "Installer publish failed"; exit 1 }

Copy-Item (Join-Path $publishDir "CloudFabRevitInstaller.exe") $distDir
Remove-Item $publishDir -Recurse -Force
Write-Host "        OK"

# ── Summary ──────────────────────────────────────────────────────
Write-Host ""
Write-Host "  Done! Distribution package:"
Write-Host "  $distDir"
Write-Host ""
Write-Host "  Contents:"
Write-Host "    CloudFabRevitInstaller.exe   <- recipient runs this"
Write-Host "    Connector\"
Write-Host "      CloudBridge.addin"
Write-Host "      CloudBridge\             ($connectorFileCount files)"
Write-Host "    Kit\"
Write-Host "      Objects.dll"
Write-Host "      Objects.Converter.Revit$RevitVersion.dll"
if (Test-Path $templatesSrc) {
  Write-Host "      Templates\Revit\$RevitVersion\"
}
Write-Host ""
Write-Host "  Zip this folder and send it. Recipient runs the exe."
Write-Host ""
