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

**Build a distribution package:**

```powershell
.\build-installer.ps1 -RevitVersion 2025   # or 2024, 2026
```

This builds ConnectorRevit + ConverterRevit in Release, gathers all files, and publishes a ~12MB self-contained installer exe into `dist\CloudFabRevit{version}\`. Zip that folder and send it.

**What the installer copies:**

| Source (next to exe) | Destination |
|---|---|
| `Connector\SpeckleRevit2.addin` | `%APPDATA%\Autodesk\Revit\Addins\{version}\` |
| `Connector\SpeckleRevit2\` (DLLs) | `%APPDATA%\Autodesk\Revit\Addins\{version}\SpeckleRevit2\` |
| `Kit\` (Objects.dll, converter, templates) | `%APPDATA%\Speckle\Kits\Objects\` |

The Revit version is baked into the exe at compile time via `AssemblyMetadataAttribute` (no config files). The `.addin` manifest uses a relative path so it works on any machine.

## Manual Step: Copy Objects.dll to Unity

After building, copy `Objects.dll` from `%APPDATA%\Speckle\Kits\Objects\Objects.dll` to the Unity project (cloudfab-unity) at **both** locations:

- `Assets\Speckle\Runtime\Objects\Objects.dll`
- `Packages\systems.speckle.speckle-unity\Runtime\Objects\Objects.dll`

## Dev Loop (repeat after each code change)

**Shell note:** Claude Code runs bash on Windows. All commands below must be invoked via `powershell.exe -NoProfile -Command '...'`. Use **single quotes** around the PowerShell command string so bash doesn't eat `$` signs (e.g. `$env:APPDATA`).

### 1. Build (local testing)

```bash
powershell.exe -NoProfile -Command 'dotnet build "C:\Users\RAMBAGE\speckle-sharp\ConnectorRevit\ConnectorRevit2025\ConnectorRevit2025.csproj" -c Debug -p:SolutionDir="C:\Users\RAMBAGE\speckle-sharp\"'
```

This builds the full chain (Connector + Converter + Objects) and the Debug post-build events auto-deploy everything to the Revit addins folder and Speckle Kits folder. No manual copy needed for Revit.

### 2. Copy Objects.dll to Unity

```bash
powershell.exe -NoProfile -Command 'Copy-Item "$env:APPDATA\Speckle\Kits\Objects\Objects.dll" "C:\Users\RAMBAGE\git\cloudfab-unity\Assets\Speckle\Runtime\Objects\Objects.dll" -Force; Copy-Item "$env:APPDATA\Speckle\Kits\Objects\Objects.dll" "C:\Users\RAMBAGE\git\cloudfab-unity\Packages\systems.speckle.speckle-unity\Runtime\Objects\Objects.dll" -Force'
```

### 3. Test

- Restart Revit (it locks the DLLs — must close and reopen)
- In Unity, enter Play mode (Unity reloads DLLs on domain reload)
- Send from one side, receive on the other, verify the data round-trips

### 4. Distribute to 3rd party

```bash
powershell.exe -NoProfile -Command 'cd "C:\Users\RAMBAGE\speckle-sharp"; .\build-installer.ps1 -RevitVersion 2025'
```

Output: `dist\CloudFabRevit2025\` — zip that folder and send it. Recipient runs `CloudFabRevitInstaller.exe`, restarts Revit, done.

## Relationship to cloudfab-unity

This repo provides the Speckle SDK that cloudfab-unity depends on. The Unity project lives at `C:\Users\RAMBAGE\git\cloudfab-unity`. The `Objects.dll` built here defines the data models (including `RevitWorkPlaneFamilyInstance`) that both Unity and Revit use to serialize/deserialize clip connectors.
