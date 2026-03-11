## Overview

Fork of [speckle-sharp](https://github.com/specklesystems/speckle-sharp) (v2/legacy). Adds `RevitWorkPlaneFamilyInstance` — a new Speckle object for sending/receiving structural connection clips (work-plane-hosted family instances) between Unity and Revit.

**Branch:** `main` (clip connector work merged from `struct5`)

## What Was Changed (3 core files + 2 support)

### Core

1. **`Objects/Objects/BuiltElements/Revit/RevitWorkPlaneFamilyInstance.cs`** (new) — Speckle data model extending `RevitInstance`. Properties: `workPlane`, `sketchPlaneUniqueId`, `facingOrientation`, `handOrientation`, `placementPoint`, `flipVertical`, etc.

2. **`Objects/Converters/ConverterRevit/ConverterRevitShared/ConverterRevit.cs`** (modified, +1567 lines) — Send path: `WorkPlaneConnectionFamilyToSpeckle()` converts `OST_StructConnections` family instances. Receive path: rewritten `RevitInstanceToNative()` with work-plane hosting, face reference resolution, vertical flip/rotation helpers. `ConvertToNative` defers clip connectors so host columns exist first.

3. **`Objects/Converters/ConverterRevit/ConverterRevitShared/PartialClasses/ConvertFamilyInstance.cs`** (modified) — Removed old `RevitInstanceToNative()` (moved to ConverterRevit.cs), cleaned up `CreateHostedFamilyInstance()`.

### Support

4. **`Objects/Converters/ConverterRevit/ConverterRevitShared/FileLogger.cs`** (new) — Debug file logger writing to `%LOCALAPPDATA%\Speckle\Logs\Revit-WP\`.
5. **`Objects/Converters/ConverterRevit/ConverterRevitShared/ConverterRevitShared.projitems`** — Added FileLogger.cs to shared project.

## Building

Open `ConnectorRevit\ConnectorRevit.sln` in Rider. Build **ConnectorRevit2025** or **ConnectorRevit2026** in **Debug** configuration.

This builds the full dependency chain (ConnectorRevit → ConverterRevit → Objects).

Supported Revit versions: 2020–2026. Each version has its own project (e.g. `ConnectorRevit2025`, `ConnectorRevit2026`).

## Auto-Deployment (post-build events)

Building ConnectorRevitXXXX in Debug automatically copies to (example for 2025):

| Component | Destination |
|---|---|
| Connector DLLs + `.addin` | `%APPDATA%\Autodesk\Revit\Addins\2025\SpeckleRevit2\` |
| `Objects.Converter.Revit2025.dll` | `%APPDATA%\Speckle\Kits\Objects\` |
| `Objects.dll` | `%APPDATA%\Speckle\Kits\Objects\` |
| Revit family templates | `%APPDATA%\Speckle\Kits\Objects\Templates\Revit\2025\` |

## ElementId Compat Shim

Revit 2026 removed `ElementId.IntegerValue`. All call sites use `id.GetIntegerValue()` — an extension method in `RevitSharedResources/Helpers/ElementIdExtensions.cs` that dispatches to `.Value` (2026) or `.IntegerValue` (older). When adding new code that reads an ElementId's numeric value, always use `GetIntegerValue()` instead of `.IntegerValue`.

## Installer (distribute to others)

`Installer/` contains a self-contained C# console app that copies the connector + kit files to the right AppData locations on a recipient's machine.

**Multi-version build (all Revit versions in one installer):**

```powershell
.\build-installer-all.ps1                          # builds 2023, 2024, 2025, 2026
.\build-installer-all.ps1 -Versions 2023,2025      # specific versions only
```

Output: `dist\CloudFabRevit\` (~156 MB zipped). Zip and send. Recipient extracts, runs `CloudFabRevitInstaller.exe`, all versions install at once.

**Single-version build (legacy):**

```powershell
.\build-installer.ps1 -RevitVersion 2025   # or 2024, 2026
```

Output: `dist\CloudFabRevit{version}\`.

**What the installer copies (per version):**

| Source (next to exe) | Destination |
|---|---|
| `Connector\SpeckleRevit2.addin` | `%APPDATA%\Autodesk\Revit\Addins\{version}\` |
| `Connector\SpeckleRevit2\` (DLLs) | `%APPDATA%\Autodesk\Revit\Addins\{version}\SpeckleRevit2\` |
| `Kit\` (Objects.dll, converter, templates) | `%APPDATA%\Speckle\Kits\Objects\` |

The installer auto-detects mode: if it sees `20XX/` subdirectories next to the exe, it installs all of them (multi-version). Otherwise falls back to baked-in `AssemblyMetadataAttribute` (single-version). Safety: only deletes stale DLLs in `SpeckleRevit2\` if our DLLs are present; never touches other Revit add-ins.

### Build script pitfalls

- **PowerShell encoding:** `.ps1` files MUST have a UTF-8 BOM or use only ASCII characters. PowerShell 5.1 on Windows reads non-BOM files as Windows-1252, corrupting em-dashes and other Unicode. Never use em-dashes (`--`) in `.ps1` files; use regular hyphens (`-`).
- **CopyToKitFolder xcopy failures:** The `Directory.Build.targets` `CopyToKitFolder` post-build target uses `xcopy` to copy DLLs to `%APPDATA%\Speckle\Kits\Objects\`. This fails with exit code 4 if Revit/Unity lock the DLLs, or if ConverterDxf hasn't been built. For Release/installer builds, pass `-p:CopyToKitFolder=false` to skip it (the build script gathers files directly from build output).
- **ConverterDxf dependency:** ConverterRevit depends on ConverterDxf. If ConverterDxf hasn't been built in Release config, the xcopy post-build step for it fails. The multi-version build script skips this via `CopyToKitFolder=false`.

## Manual Step: Copy Objects.dll to Unity

After building, copy `Objects.dll` from `%APPDATA%\Speckle\Kits\Objects\Objects.dll` to the Unity project (cloudfab-unity) at **both** locations:

- `Assets\Speckle\Runtime\Objects\Objects.dll`
- `Packages\systems.speckle.speckle-unity\Runtime\Objects\Objects.dll`

## Dev Loop (repeat after each code change)

**Shell note:** Claude Code runs bash on Windows. All commands below must be invoked via `powershell.exe -NoProfile -Command '...'`. Use **single quotes** around the PowerShell command string so bash doesn't eat `$` signs (e.g. `$env:APPDATA`).

### 1. Build (local testing)

**Important: ConnectorRevit2025 does NOT build ConverterRevit2025.** They are separate projects. You must build both.

**Close Revit AND Unity before building.** Both lock DLLs in `%APPDATA%\Speckle\Kits\Objects\` — Revit locks the converter DLLs, Unity locks them via its Speckle modules. The `CopyToKitFolder` post-build xcopy will fail with exit code 4 ("user-mapped section open") if either is running.

```bash
# Step 1: Build the Connector (deploys to Revit addins folder)
powershell.exe -NoProfile -Command 'dotnet build "C:\Users\RAMBAGE\speckle-sharp\ConnectorRevit\ConnectorRevit2025\ConnectorRevit2025.csproj" -c Debug -p:SolutionDir="C:\Users\RAMBAGE\speckle-sharp\"'

# Step 2: Build the Converter (deploys to Speckle Kits folder)
# Use CopyToKitFolder=false if the xcopy post-build still fails, then manually copy.
powershell.exe -NoProfile -Command 'dotnet build "C:\Users\RAMBAGE\speckle-sharp\Objects\Converters\ConverterRevit\ConverterRevit2025\ConverterRevit2025.csproj" -c Debug -p:SolutionDir="C:\Users\RAMBAGE\speckle-sharp\" --no-incremental'
```

If the converter build fails on the DxfConverter xcopy (exit code 4), bypass it:
```bash
powershell.exe -NoProfile -Command 'dotnet build "C:\Users\RAMBAGE\speckle-sharp\Objects\Converters\ConverterRevit\ConverterRevit2025\ConverterRevit2025.csproj" -c Debug -p:SolutionDir="C:\Users\RAMBAGE\speckle-sharp\" -p:CopyToKitFolder=false --no-incremental'
# Then manually deploy:
powershell.exe -NoProfile -Command 'Copy-Item "C:\Users\RAMBAGE\speckle-sharp\Objects\Converters\ConverterRevit\ConverterRevit2025\bin\Debug\net8.0-windows\Objects.Converter.Revit2025.dll" "$env:APPDATA\Speckle\Kits\Objects\" -Force'
```

**Verify timestamps after build** — if the converter DLL timestamp didn't change, it wasn't rebuilt. Use `--no-incremental` to force recompilation.

### 2. Copy Objects.dll to Unity

```bash
powershell.exe -NoProfile -Command 'Copy-Item "$env:APPDATA\Speckle\Kits\Objects\Objects.dll" "C:\Users\RAMBAGE\git\cloudfab-unity\Assets\Speckle\Runtime\Objects\Objects.dll" -Force; Copy-Item "$env:APPDATA\Speckle\Kits\Objects\Objects.dll" "C:\Users\RAMBAGE\git\cloudfab-unity\Packages\systems.speckle.speckle-unity\Runtime\Objects\Objects.dll" -Force'
```

### 3. Test

- Restart Revit (it locks the DLLs — must close and reopen)
- In Unity, enter Play mode (Unity reloads DLLs on domain reload)
- Send from one side, receive on the other, verify the data round-trips
- Check converter logs at `%LOCALAPPDATA%\Speckle\Logs\Revit-WP\` for `[RVTIN]` messages

### 4. Distribute to 3rd party

```bash
# All versions (2023-2026) in one installer:
powershell.exe -NoProfile -Command 'cd "C:\Users\RAMBAGE\speckle-sharp"; .\build-installer-all.ps1'

# Single version:
powershell.exe -NoProfile -Command 'cd "C:\Users\RAMBAGE\speckle-sharp"; .\build-installer.ps1 -RevitVersion 2025'
```

Output: `dist\CloudFabRevit\` (multi) or `dist\CloudFabRevit2025\` (single). Zip and send. Recipient runs `CloudFabRevitInstaller.exe`, restarts Revit, done.

## Connection Rotation & Flip (Unity → Revit)

Connections are hosted on a ReferencePlane + SketchPlane (plane-hosted, NOT element/face-hosted). The rotation/flip pipeline spans both repos.

### `manualAngles` (per-prefab mesh offset)

`CloudFabProperties.manualAngles` (`Vector3`) is a per-prefab mesh offset. Each clip prefab's mesh vertices are pre-rotated differently inside the prefab — even when the Unity transform is `(0,0,0)` the mesh faces an arbitrary direction. `manualAngles` must be removed before extracting yaw for Revit.

### Send side (cloudfab-unity, `DebugHosting.SetConnectionProps`)

**Rotation + flip (current field-validated mapping):**
```
q = unityRotation * inverse(euler(manualAngles))
if isTop:
    q = q * angleAxis(-180deg, X)      // remove Unity top flip before yaw extraction

baseYaw = deltaAngle(q.eulerY)
rotation = -baseYaw * deg2rad + PI
flipVertical = !isTop
```
- Quaternion removal of `manualAngles` is more reliable than Euler Y subtraction when top/bottom flips are involved.
- The +PI yaw compensation is currently applied for all clips in the active family set.
- `flipVertical` is intentionally inverted (`!isTop`) for current clips; this matches Revit visual results for jamb + bridging clips in real sends.
- `isTop` edits made after framing are not sent unless `SetConnectionProps()` is run again (payload is prepared in `Prepare2Revit()`).

**Host ID:** `sp.Data["connectedElementId"] = colId` where `colId = AppIdUtility.GetOrCreate(col.gameObject)` (format: `UNITY-{guid}`). This was accidentally removed in commit `71240bd` and must be present.

### Receive side (this repo, `ConverterRevit.RevitInstanceToNative`)

- Reads `rotation` from `instance["rotation"]` (dynamic double, since Unity sends `RevitInstance` not `RevitWorkPlaneFamilyInstance`)
- Uses `rotation` directly as the ReferencePlane yaw — no additional receive-side offset.
- Reads `flipVertical` via `WantsVerticalFlip()` → applies vertical mirror via `MirrorElements` across horizontal plane (normal=BasisZ) at the insertion point
- After VFlip, hand/facing/mirror corrections are skipped (`vflipApplied` guard) to prevent undoing the mirror

### Log-first validation workflow

Use `%LOCALAPPDATA%\Speckle\Logs\Revit-WP\<latest>\log.txt` and check:
- `[RVTIN] instance:... fam:'...' ... yaw(rad):...`
- `[RVTIN] flipVertical=True|False ...`

For quick checks, group by family and count True/False to verify payload mapping before debugging visual orientation.

## Screw Rotation & Elevation (Unity → Revit)

Screws are now `Structural Connections` (not `Generic Models`) and flow through the same `SetConnectionProps` / `RevitInstanceToNative` pipeline as clips. They are WorkPlaneBased `RevitInstance` objects — no host ID, no flip, no manualAngles.

**Family requirement:** The Revit screw family must have "Work-Plane-Based" checked in the Family Editor. Without this, elevation from host stays 0.

### Rotation (3 cases)

The converter distinguishes screws from clips via `connectedElementId == null` → applies `RotateElement` instead of ReferencePlane direction encoding.

1. **Default (horizontal):** `RotateElement` around BasisZ by yaw. ReferencePlane is created with yaw=0 (hosting only).
2. **faceUp:** `TryRotateAngle(InPlaneX, +PI/2)` tilts screw 90° upward. Yaw is skipped (`faceTilted` flag).
3. **faceDown:** `TryRotateAngle(InPlaneX, -PI/2)` tilts screw 90° downward. Yaw is skipped.

`TryRotateAngle` is a generalized version of `TryRotate90` that accepts an arbitrary angle. `TryRotate90` still exists as a wrapper.

**Why RotateElement for default yaw:** Screws typically have only 0° and 180° yaw (facing opposite sides of a stud). These values produce geometrically equivalent ReferencePlane lines (±Y), so the plane approach can't distinguish them.

### Elevation

`INSTANCE_ELEVATION_PARAM` is set explicitly via `get_Parameter()` after `SetInstanceParameters`. The generic `SetInstanceParameters` path skips it because the `ParametersMap` `IsReadOnly` filter excludes it for WorkPlaneBased instances.

## Startup Popup

`ConnectorRevit/Entry/App.cs` shows both Connector and Converter build timestamps on startup. Converter path is resolved via `SpecklePathProvider.InstallApplicationDataPath + /Kits/Objects/Objects.Converter.Revit2025.dll`.

## Relationship to cloudfab-unity

This repo provides the Speckle SDK that cloudfab-unity depends on. The Unity project lives at `C:\Users\RAMBAGE\git\cloudfab-unity`. The `Objects.dll` built here defines the data models (including `RevitWorkPlaneFamilyInstance`) that both Unity and Revit use to serialize/deserialize clip connectors.
