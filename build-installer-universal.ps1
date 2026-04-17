# CloudFab Universal Connector - Build & Package ALL Connectors
#
# Usage:
#   .\build-installer-universal.ps1                            # build everything
#   .\build-installer-universal.ps1 -RevitVersions 2025,2026  # specific Revit versions
#   .\build-installer-universal.ps1 -Skip AutoCAD,Civil3D     # skip certain connectors
#
# Output: dist\CloudFab\  (zip this folder and send to recipient)

param(
  [string[]]$RevitVersions      = @("2023", "2024", "2025", "2026"),
  [string[]]$AutoCADVersions    = @("2021", "2022", "2023", "2024", "2025"),
  [string[]]$Civil3DVersions    = @("2021", "2022", "2023", "2024", "2025"),
  [string[]]$RhinoVersions      = @("7", "8"),
  [string[]]$GrasshopperVersions= @("7", "8"),
  [string[]]$NavisworksVersions = @("2020", "2021", "2022", "2023", "2024", "2025"),
  [switch]$IncludeDynamo,
  [string[]]$Skip = @()           # connector names to skip entirely
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

Write-Host ""
Write-Host "  CloudFab Universal Connector - Build & Package"
Write-Host "  ================================================"
Write-Host ""

# ── Paths ────────────────────────────────────────────────────────
$installerProj = Join-Path $root "Installer\CloudFabRevitInstaller.csproj"
$distDir       = Join-Path $root "dist\CloudFab"

if (-not (Test-Path $installerProj)) {
  Write-Error "Installer project not found: $installerProj"
  exit 1
}

# ── Clean dist ───────────────────────────────────────────────────
if (Test-Path $distDir) { Remove-Item $distDir -Recurse -Force }
New-Item $distDir -ItemType Directory | Out-Null

$built = @()
$skipped = @()
$failed = @()

# ── Helper: find main assembly in bin tree after build ───────────
function Find-OutputDir($projDir, $assemblyName, $extension = "dll") {
  $binDir = Join-Path $projDir "bin"
  if (-not (Test-Path $binDir)) { return $null }
  $found = Get-ChildItem $binDir -Recurse -Filter "$assemblyName.$extension" -ErrorAction SilentlyContinue |
    Where-Object { $_.DirectoryName -match "[/\\]Release[/\\]?" } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
  if ($found) { return $found.DirectoryName }
  return $null
}

# ══════════════════════════════════════════════════════════════════
# REVIT
# ══════════════════════════════════════════════════════════════════
if ($Skip -notcontains "Revit") {
  $revitSolutionDir = Join-Path $root "ConnectorRevit"

  foreach ($ver in $RevitVersions) {
    $label = "Revit $ver"
    $connProj = Join-Path $revitSolutionDir "ConnectorRevit$ver\ConnectorRevit$ver.csproj"
    $convProj = Join-Path $root "Objects\Converters\ConverterRevit\ConverterRevit$ver\ConverterRevit$ver.csproj"

    if (-not (Test-Path $connProj)) { $skipped += "$label (no project)"; continue }

    Write-Host "  Building $label..."

    # Build connector
    dotnet build $connProj -c Release "-p:SolutionDir=$revitSolutionDir\" -p:IsDesktopBuild=false -p:EnforceCodeStyleInBuild=false -p:TreatWarningsAsErrors=false --nologo -v q
    if ($LASTEXITCODE -ne 0) { $failed += $label; Write-Warning "  $label connector build failed"; continue }

    # Build converter
    if (Test-Path $convProj) {
      dotnet build $convProj -c Release -p:CopyToKitFolder=false -p:EnforceCodeStyleInBuild=false -p:TreatWarningsAsErrors=false --nologo -v q
      if ($LASTEXITCODE -ne 0) { Write-Warning "  $label converter build failed (continuing)" }
    }

    # Gather connector files
    $verDir = Join-Path $distDir "Revit\$ver"
    $connDist = Join-Path $verDir "Connector"
    New-Item $connDist -ItemType Directory -Force | Out-Null

    $releaseDir = Join-Path $revitSolutionDir "Release\Release$ver"
    if (-not (Test-Path $releaseDir)) { $releaseDir = Join-Path $root "Release\Release$ver" }

    if (Test-Path $releaseDir) {
      $addinFile = Join-Path $releaseDir "CloudBridge.addin"
      if (Test-Path $addinFile) { Copy-Item $addinFile $connDist }
      $cbSrc = Join-Path $releaseDir "CloudBridge"
      if (Test-Path $cbSrc) { Copy-Item $cbSrc (Join-Path $connDist "CloudBridge") -Recurse }
    } else {
      $binDir = Join-Path $revitSolutionDir "ConnectorRevit$ver\bin\Release\win-x64"
      $addinFile = Get-ChildItem $binDir -Filter "CloudBridge.addin" -ErrorAction SilentlyContinue | Select-Object -First 1
      if ($addinFile) {
        Copy-Item $addinFile.FullName $connDist
        $cbDist = Join-Path $connDist "CloudBridge"
        New-Item $cbDist -ItemType Directory -Force | Out-Null
        Copy-Item "$binDir\*" $cbDist -Recurse -Exclude "*.addin"
      } else {
        $failed += $label; Write-Warning "  $label - CloudBridge.addin not found"; continue
      }
    }

    # Gather Kit files
    $kitDist = Join-Path $verDir "Kit"
    New-Item $kitDist -ItemType Directory -Force | Out-Null

    $objectsDll = Get-ChildItem (Join-Path $root "Objects\Objects\bin\Release") -Recurse -Filter "Objects.dll" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($objectsDll) { Copy-Item $objectsDll.FullName $kitDist }

    $converterDll = Get-ChildItem (Join-Path $root "Objects\Converters\ConverterRevit\ConverterRevit$ver\bin\Release") -Recurse -Filter "Objects.Converter.Revit$ver.dll" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($converterDll) { Copy-Item $converterDll.FullName $kitDist }

    $templatesSrc = Join-Path $root "Objects\Converters\ConverterRevit\Templates\$ver"
    if (Test-Path $templatesSrc) {
      $templatesDist = Join-Path $kitDist "Templates\Revit\$ver"
      New-Item $templatesDist -ItemType Directory -Force | Out-Null
      Copy-Item "$templatesSrc\*" $templatesDist
    }

    $built += $label
    Write-Host "        $label OK"
  }
}

# ══════════════════════════════════════════════════════════════════
# AUTOCAD
# ══════════════════════════════════════════════════════════════════
if ($Skip -notcontains "AutoCAD") {
  foreach ($ver in $AutoCADVersions) {
    $label = "AutoCAD $ver"
    $projDir = Join-Path $root "ConnectorAutocadCivil\ConnectorAutocad$ver"
    $csproj  = Join-Path $projDir "ConnectorAutocad$ver.csproj"

    if (-not (Test-Path $csproj)) { $skipped += "$label (no project)"; continue }

    Write-Host "  Building $label..."
    dotnet build $csproj -c Release -p:IsDesktopBuild=false -p:EnforceCodeStyleInBuild=false -p:TreatWarningsAsErrors=false --nologo -v q
    if ($LASTEXITCODE -ne 0) { $failed += $label; Write-Warning "  $label build failed"; continue }

    $outputDir = Find-OutputDir $projDir "SpeckleConnectorAutocad"
    if (-not $outputDir) { $failed += $label; Write-Warning "  $label - output not found"; continue }

    $dest = Join-Path $distDir "AutoCAD\$ver"
    New-Item $dest -ItemType Directory -Force | Out-Null
    Copy-Item "$outputDir\*" $dest -Recurse
    $built += $label
    Write-Host "        $label OK"
  }
}

# ══════════════════════════════════════════════════════════════════
# CIVIL 3D
# ══════════════════════════════════════════════════════════════════
if ($Skip -notcontains "Civil3D") {
  foreach ($ver in $Civil3DVersions) {
    $label = "Civil 3D $ver"
    $projDir = Join-Path $root "ConnectorAutocadCivil\ConnectorCivil$ver"
    $csproj  = Join-Path $projDir "ConnectorCivil$ver.csproj"

    if (-not (Test-Path $csproj)) { $skipped += "$label (no project)"; continue }

    Write-Host "  Building $label..."
    dotnet build $csproj -c Release -p:IsDesktopBuild=false -p:EnforceCodeStyleInBuild=false -p:TreatWarningsAsErrors=false --nologo -v q
    if ($LASTEXITCODE -ne 0) { $failed += $label; Write-Warning "  $label build failed"; continue }

    $outputDir = Find-OutputDir $projDir "SpeckleConnectorCivil"
    if (-not $outputDir) { $failed += $label; Write-Warning "  $label - output not found"; continue }

    $dest = Join-Path $distDir "Civil3D\$ver"
    New-Item $dest -ItemType Directory -Force | Out-Null
    Copy-Item "$outputDir\*" $dest -Recurse
    $built += $label
    Write-Host "        $label OK"
  }
}

# ══════════════════════════════════════════════════════════════════
# RHINO
# ══════════════════════════════════════════════════════════════════
if ($Skip -notcontains "Rhino") {
  foreach ($ver in $RhinoVersions) {
    $label = "Rhino $ver"
    $projDir = Join-Path $root "ConnectorRhino\ConnectorRhino$ver"
    $csproj  = Join-Path $projDir "ConnectorRhino$ver.csproj"

    if (-not (Test-Path $csproj)) { $skipped += "$label (no project)"; continue }

    Write-Host "  Building $label..."
    dotnet build $csproj -c Release -p:IsDesktopBuild=false -p:EnforceCodeStyleInBuild=false -p:TreatWarningsAsErrors=false --nologo -v q
    if ($LASTEXITCODE -ne 0) { $failed += $label; Write-Warning "  $label build failed"; continue }

    $outputDir = Find-OutputDir $projDir "SpeckleConnectorRhino" "rhp"
    if (-not $outputDir) { $failed += $label; Write-Warning "  $label - output not found"; continue }

    $dest = Join-Path $distDir "Rhino\$ver"
    New-Item $dest -ItemType Directory -Force | Out-Null
    Copy-Item "$outputDir\*" $dest -Recurse
    $built += $label
    Write-Host "        $label OK"
  }
}

# ══════════════════════════════════════════════════════════════════
# GRASSHOPPER
# ══════════════════════════════════════════════════════════════════
if ($Skip -notcontains "Grasshopper") {
  foreach ($ver in $GrasshopperVersions) {
    $label = "Grasshopper $ver"
    $projDir = Join-Path $root "ConnectorGrasshopper\ConnectorGrasshopper$ver"
    $csproj  = Join-Path $projDir "ConnectorGrasshopper$ver.csproj"

    if (-not (Test-Path $csproj)) { $skipped += "$label (no project)"; continue }

    Write-Host "  Building $label..."
    dotnet build $csproj -c Release -p:IsDesktopBuild=false -p:EnforceCodeStyleInBuild=false -p:TreatWarningsAsErrors=false --nologo -v q
    if ($LASTEXITCODE -ne 0) { $failed += $label; Write-Warning "  $label build failed"; continue }

    $outputDir = Find-OutputDir $projDir "SpeckleConnectorGrasshopper" "gha"
    if (-not $outputDir) { $failed += $label; Write-Warning "  $label - output not found"; continue }

    $dest = Join-Path $distDir "Grasshopper\$ver"
    New-Item $dest -ItemType Directory -Force | Out-Null
    Copy-Item "$outputDir\*" $dest -Recurse
    $built += $label
    Write-Host "        $label OK"
  }
}

# ══════════════════════════════════════════════════════════════════
# NAVISWORKS
# ══════════════════════════════════════════════════════════════════
if ($Skip -notcontains "Navisworks") {
  $navSolutionDir = Join-Path $root "ConnectorNavisworks"
  $navPackageXml = Join-Path $navSolutionDir "ConnectorNavisworks\Entry\PackageContents.xml"

  foreach ($ver in $NavisworksVersions) {
    $label = "Navisworks $ver"
    $projDir = Join-Path $navSolutionDir "ConnectorNavisworks$ver"
    $csproj  = Join-Path $projDir "ConnectorNavisworks$ver.csproj"

    if (-not (Test-Path $csproj)) { $skipped += "$label (no project)"; continue }

    Write-Host "  Building $label..."
    dotnet build $csproj -c Release "-p:SolutionDir=$navSolutionDir\" -p:IsDesktopBuild=false -p:EnforceCodeStyleInBuild=false -p:TreatWarningsAsErrors=false --nologo -v q
    if ($LASTEXITCODE -ne 0) { $failed += $label; Write-Warning "  $label build failed"; continue }

    # Prefer the Release staging dir (populated by PostBuild xcopy)
    $releaseDir = Join-Path $navSolutionDir "Release\Release$ver"
    if (Test-Path $releaseDir) {
      $outputDir = $releaseDir
    } else {
      $outputDir = Find-OutputDir $projDir "SpeckleConnectorNavisworks"
    }
    if (-not $outputDir) { $failed += $label; Write-Warning "  $label - output not found"; continue }

    $dest = Join-Path $distDir "Navisworks\$ver"
    New-Item $dest -ItemType Directory -Force | Out-Null
    Copy-Item "$outputDir\*" $dest -Recurse -Exclude "Entry"

    # Ribbon files from Entry/ in build output
    $entryDir = Join-Path $outputDir "Entry"
    if (-not (Test-Path $entryDir)) {
      # Entry might be in the original build output, not the staging dir
      $binEntryDir = Join-Path (Find-OutputDir $projDir "SpeckleConnectorNavisworks") "Entry" -ErrorAction SilentlyContinue
      if ($binEntryDir -and (Test-Path $binEntryDir)) { $entryDir = $binEntryDir }
    }
    if (Test-Path $entryDir) {
      $ribbonFiles = Get-ChildItem $entryDir -Filter "Ribbon.*" -ErrorAction SilentlyContinue
      if ($ribbonFiles) {
        $enUs = Join-Path $dest "en-US"
        New-Item $enUs -ItemType Directory -Force | Out-Null
        foreach ($f in $ribbonFiles) { Copy-Item $f.FullName $enUs }
      }
    }

    $built += $label
    Write-Host "        $label OK"
  }

  # Copy PackageContents.xml to Navisworks dist root
  $navDist = Join-Path $distDir "Navisworks"
  if ((Test-Path $navDist) -and (Test-Path $navPackageXml)) {
    Copy-Item $navPackageXml $navDist
    Write-Host "        PackageContents.xml copied"
  }
}

# ══════════════════════════════════════════════════════════════════
# DYNAMO
# ══════════════════════════════════════════════════════════════════
if ($IncludeDynamo -and $Skip -notcontains "Dynamo") {
  $label = "Dynamo"
  $dynamoProj = Join-Path $root "ConnectorDynamo\ConnectorDynamo\ConnectorDynamo.csproj"

  if (-not (Test-Path $dynamoProj)) {
    $skipped += "$label (no project)"
  } else {
    Write-Host "  Building $label..."
    dotnet build $dynamoProj -c Release -p:IsDesktopBuild=false -p:EnforceCodeStyleInBuild=false -p:TreatWarningsAsErrors=false --nologo -v q
    if ($LASTEXITCODE -ne 0) {
      $failed += $label; Write-Warning "  $label build failed"
    } else {
      $dynamoDist = Join-Path $root "ConnectorDynamo\ConnectorDynamo\dist\SpeckleDynamo2"
      if (Test-Path $dynamoDist) {
        $dest = Join-Path $distDir "Dynamo"
        Copy-Item $dynamoDist $dest -Recurse
        $built += $label
        Write-Host "        $label OK"
      } else {
        $failed += $label; Write-Warning "  $label - dist output not found"
      }
    }
  }
}

# ══════════════════════════════════════════════════════════════════
# SHARED OBJECTS KIT (converters for non-Revit connectors)
# ══════════════════════════════════════════════════════════════════
$kitDist = Join-Path $distDir "Kit"
$hasKitFiles = $false

# Objects.dll (shared by all)
$objectsDll = Get-ChildItem (Join-Path $root "Objects\Objects\bin\Release") -Recurse -Filter "Objects.dll" -ErrorAction SilentlyContinue | Select-Object -First 1
if ($objectsDll) {
  New-Item $kitDist -ItemType Directory -Force | Out-Null
  Copy-Item $objectsDll.FullName $kitDist
  $hasKitFiles = $true
}

# Converter DLLs for non-Revit connectors (Revit has its own per-version Kit)
$converterPatterns = @(
  @{ Path = "Objects\Converters\ConverterRhinoGh"; Pattern = "Objects.Converter.RhinoGh*.dll" },
  @{ Path = "Objects\Converters\ConverterAutocadCivil"; Pattern = "Objects.Converter.AutocadCivil*.dll" },
  @{ Path = "Objects\Converters\ConverterNavisworks"; Pattern = "Objects.Converter.Navisworks*.dll" },
  @{ Path = "Objects\Converters\ConverterDynamo"; Pattern = "Objects.Converter.Dynamo*.dll" }
)

foreach ($conv in $converterPatterns) {
  $convDir = Join-Path $root $conv.Path
  if (-not (Test-Path $convDir)) { continue }
  $dlls = Get-ChildItem $convDir -Recurse -Filter $conv.Pattern -ErrorAction SilentlyContinue |
    Where-Object { $_.DirectoryName -match "Release" }
  foreach ($dll in $dlls) {
    New-Item $kitDist -ItemType Directory -Force | Out-Null
    Copy-Item $dll.FullName $kitDist -ErrorAction SilentlyContinue
    $hasKitFiles = $true
  }
}

if ($hasKitFiles) {
  $kitCount = (Get-ChildItem $kitDist -Recurse -File).Count
  Write-Host "  Kit: gathered $kitCount shared files"
}

# ══════════════════════════════════════════════════════════════════
# PUBLISH INSTALLER EXE
# ══════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "  Publishing installer exe..."
$publishDir = Join-Path $distDir "__publish"
dotnet publish $installerProj -c Release -o $publishDir --nologo -v q
if ($LASTEXITCODE -ne 0) { Write-Error "Installer publish failed"; exit 1 }

Copy-Item (Join-Path $publishDir "CloudFabInstaller.exe") $distDir
Remove-Item $publishDir -Recurse -Force
Write-Host "  OK"

# ══════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "  ================================================"
Write-Host "  Build Summary"
Write-Host "  ================================================"
Write-Host ""

if ($built.Count -gt 0) {
  Write-Host "  Built successfully ($($built.Count)):"
  foreach ($b in $built) { Write-Host "    [OK] $b" }
}

if ($skipped.Count -gt 0) {
  Write-Host ""
  Write-Host "  Skipped ($($skipped.Count)):"
  foreach ($s in $skipped) { Write-Host "    [--] $s" }
}

if ($failed.Count -gt 0) {
  Write-Host ""
  Write-Host ""
  Write-Host "  Failed ($($failed.Count)):" -ForegroundColor Yellow
  foreach ($f in $failed) { Write-Host "    [!!] $f" }
  Write-Host ""
  Write-Host "  Failed connectors may need specific SDKs installed."
}

Write-Host ""
Write-Host "  Output: $distDir"
Write-Host ""
Write-Host "  Contents:"
Write-Host "    CloudFabInstaller.exe   <- recipient runs this"

$connectorDirs = Get-ChildItem $distDir -Directory | Where-Object { $_.Name -ne "__publish" }
foreach ($dir in $connectorDirs) {
  $subDirs = Get-ChildItem $dir.FullName -Directory -ErrorAction SilentlyContinue
  if ($subDirs) {
    $versions = ($subDirs | ForEach-Object { $_.Name }) -join ", "
    Write-Host "    $($dir.Name)\  ($versions)"
  } else {
    Write-Host "    $($dir.Name)\"
  }
}

Write-Host ""
Write-Host "  Zip this folder and send it. Recipient runs CloudFabInstaller.exe."
Write-Host ""
