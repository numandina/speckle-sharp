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
6. **`Objects/Converters/ConverterRevit/ConverterRevitShared/PartialClasses/ConvertBeam.cs`** (modified) — `BeamToNative` always calls `DisallowJoinAtEnd` on both ends for near-vertical baselines (|normalized Z| > 0.9), regardless of the `disallow-join` user setting. Unity sends studs as `RevitBeam`/`RevitBrace` on vertical lines (see cloudfab-unity CLAUDE.md § "Studs as Structural Framing"); without this, Revit auto-joins them into the tracks.

## Version Bump (every change)

**Bump the version in `Directory.Build.props` on every change to the plugin.** The version is shown in the Revit ribbon header (e.g. `CLOUDBRIDGE for Revit 2025 v2.6.0.0`) and is the only signal users have that they're running a new build. If you forget to bump, recipients of the installer cannot tell which version they have.

- Edit both `Version` and `FileVersion` in `Directory.Build.props` (lines ~109–110). Keep them in sync — `AssemblyVersion` derives from `FileVersion`, and `Bindings.ConnectorVersion` reads the assembly's `Version` (so the UI shows `FileVersion`).
- Scheme: `Major.Minor.Patch` for `Version`, `Major.Minor.Patch.Build` for `FileVersion`. Bump patch for fixes/small changes, minor for features, major for breaking changes.
- Current: `2.6.0` / `2.6.0.0`. (Historical placeholder was `2.0.999.x` — never incremented; do not regress to that.)

## Building

Open `ConnectorRevit\ConnectorRevit.sln` in Rider. Build **ConnectorRevit2025** or **ConnectorRevit2026** in **Debug** configuration.

This builds the full dependency chain (ConnectorRevit → ConverterRevit → Objects).

Supported Revit versions: 2020–2026. Each version has its own project (e.g. `ConnectorRevit2025`, `ConnectorRevit2026`).

## Auto-Deployment (post-build events)

Building ConnectorRevitXXXX in Debug automatically copies to (example for 2025):

| Component | Destination |
|---|---|
| Connector DLLs + `.addin` | `%APPDATA%\Autodesk\Revit\Addins\2025\CloudBridge\` |
| `Objects.Converter.Revit2025.dll` | `%APPDATA%\Speckle\Kits\Objects\` |
| `Objects.dll` | `%APPDATA%\Speckle\Kits\Objects\` |
| Revit family templates | `%APPDATA%\Speckle\Kits\Objects\Templates\Revit\2025\` |

## ElementId Compat Shim

Revit 2026 removed `ElementId.IntegerValue`. All call sites use `id.GetIntegerValue()` — an extension method in `RevitSharedResources/Helpers/ElementIdExtensions.cs` that dispatches to `.Value` (2026) or `.IntegerValue` (older). When adding new code that reads an ElementId's numeric value, always use `GetIntegerValue()` instead of `.IntegerValue`.

## Installer (distribute to others)

`Installer/` contains a self-contained C# console app (`CloudFabInstaller.exe`) that copies connector + kit files to the right AppData locations on a recipient's machine.

### Universal build (all connectors)

```powershell
.\build-installer-universal.ps1                            # build everything (except Dynamo)
.\build-installer-universal.ps1 -Skip AutoCAD,Civil3D      # skip certain connectors
.\build-installer-universal.ps1 -IncludeDynamo              # include Dynamo (needs local DLL)
.\build-installer-universal.ps1 -RevitVersions 2025,2026   # specific Revit versions only
```

Output: `dist\CloudFab\`. Zip and send. Recipient runs `CloudFabInstaller.exe`.

**Supported connectors and install targets:**

| Connector | Versions | Install Path |
|---|---|---|
| Revit | 2023–2026 | `%APPDATA%\Autodesk\Revit\Addins\{year}\` |
| AutoCAD | 2021–2025 | `%APPDATA%\Autodesk\ApplicationPlugins\Speckle2AutoCAD{year}\` |
| Civil 3D | 2021–2025 | `%APPDATA%\Autodesk\ApplicationPlugins\Speckle2Civil3D{year}\` |
| Rhino | 7, 8 | `%APPDATA%\McNeel\Rhinoceros\{ver}.0\Plug-ins\SpeckleConnectorRhino\` |
| Grasshopper | 7, 8 | `%APPDATA%\Grasshopper\Libraries\SpeckleConnectorGrasshopper{ver}\` |
| Navisworks | 2020–2025 | `%APPDATA%\Autodesk\ApplicationPlugins\Speckle.ConnectorNavisworks.bundle\` |
| Dynamo | single | `%APPDATA%\Dynamo\Dynamo Revit\*\packages\SpeckleDynamo2\` (scans existing installs) |
| Objects Kit | shared | `%APPDATA%\Speckle\Kits\Objects\` |

The installer auto-detects what's in the dist folder: connector-type folders (`Revit/`, `AutoCAD/`, etc.) with version subfolders. Also backward-compatible with the old Revit-only layout (year folders directly next to exe). Connectors that fail to build are skipped — the installer only installs what's present.

### Revit-only build

```powershell
.\build-installer-all.ps1                          # builds Revit 2023–2026
.\build-installer-all.ps1 -Versions 2023,2025      # specific versions only
.\build-installer.ps1 -RevitVersion 2025            # single version (legacy)
```

Output: `dist\CloudFabRevit\` (multi) or `dist\CloudFabRevit{version}\` (single).

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
# Step 1: Build the Connector (should auto-deploy to Revit addins folder)
powershell.exe -NoProfile -Command 'dotnet build "C:\Users\RAMBAGE\speckle-sharp\ConnectorRevit\ConnectorRevit2025\ConnectorRevit2025.csproj" -c Debug -p:SolutionDir="C:\Users\RAMBAGE\speckle-sharp\" -p:IsDesktopBuild=true'

# Step 1b (auto-deploy fallback): The AfterBuildDebug MSBuild target sometimes silently skips
# — DLL gets built in bin\Debug\win-x64\ but nothing copies to %APPDATA%\Autodesk\Revit\Addins\2025\.
# Passing -p:IsDesktopBuild=true does NOT reliably fix it. If the Addins folder is empty or stale
# after a successful build, copy manually:
powershell.exe -NoProfile -Command '$src="C:\Users\RAMBAGE\speckle-sharp\ConnectorRevit\ConnectorRevit2025\bin\Debug\win-x64"; $dst="$env:APPDATA\Autodesk\Revit\Addins\2025"; if(!(Test-Path "$dst\CloudBridge")){New-Item -ItemType Directory -Path "$dst\CloudBridge" -Force | Out-Null}; Copy-Item "$src\*" "$dst\CloudBridge" -Recurse -Force -Exclude "*.addin"; Copy-Item "$src\*.addin" $dst -Force'

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
# All connectors (Revit, AutoCAD, Civil 3D, Rhino, Grasshopper, Navisworks):
powershell.exe -NoProfile -Command 'cd "C:\Users\RAMBAGE\speckle-sharp"; .\build-installer-universal.ps1'

# Revit only (all versions 2023-2026):
powershell.exe -NoProfile -Command 'cd "C:\Users\RAMBAGE\speckle-sharp"; .\build-installer-all.ps1'

# Revit single version:
powershell.exe -NoProfile -Command 'cd "C:\Users\RAMBAGE\speckle-sharp"; .\build-installer.ps1 -RevitVersion 2025'
```

Output: `dist\CloudFab\` (universal), `dist\CloudFabRevit\` (Revit multi), or `dist\CloudFabRevit2025\` (single). Zip and send. Recipient runs `CloudFabInstaller.exe`, restarts apps, done.

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

Screws are `Structural Connections` and flow through the same `SetConnectionProps` / `RevitInstanceToNative` pipeline as clips, identified by `connectedElementId == null` (`hostlessWpb`). No host ID, no flip, no manualAngles.

**Family requirement:** the Revit screw family must have **Work Plane-Based** checked in the Family Editor (Family Category and Parameters). The payload's `placementType` is only Unity's claim — Revit obeys the family's own checkbox. The converter reads the real `Family.FamilyPlacementType` per instance (logged as `realFamPlacement:`) and branches:

### Work-plane-based family (full behavior — position + yaw + tilt)

A geometric `SketchPlane` (`Plane.CreateByOriginAndBasis`) is created exactly through the placement point and passed as host. The family frame maps family-X→basisX, family-Y→basisY, family-Z→normal; **the screw's shank is authored along family-X** (field-calibrated 2026-06-12):

1. **Plain yaw:** horizontal plane, basis = default frame rotated about Z by yaw. Reproduces "place at default + rotate in plan" exactly.
2. **faceUp:** `basisX = +Z` (shank straight up). 3. **faceDown:** `basisX = −Z`.

No `RotateElement` afterwards (rotating a WPB instance out of its plane → Revit deletes it **silently at commit**, after the last log line). Hand/facing/mirror corrections are skipped (payload flags are stale prefab captures). `INSTANCE_FREE_HOST_OFFSET_PARAM` / `INSTANCE_ELEVATION_PARAM` are forced to 0 after `SetInstanceParameters` — the plane already passes through the exact point.

### Level-based family (fallback — position + yaw only, tilt impossible)

If the family is not work-plane-based, plane hosts are silently dropped and the instance re-hosts on the level at elevation 0. The converter instead places on the level and sets **Offset-from-Host = point Z − level elevation** (after `SetInstanceParameters`), then yaws via `RotateElement` about Z. **faceUp/faceDown cannot work here:** rotation about any horizontal axis "succeeds" via API but the level constraint re-flattens the instance at regenerate — verified empirically with two different axes.

### Duplicate screws (Unity side)

Coincident same-family screws (back-to-back clips sharing a hole position) arrive as identical instances and Revit silently deletes one of each pair (observed: 416 sent → 372 surviving). `NewEngine.Prepare2Revit` dedupes them before export (1mm position quantization + family name).

## Startup Popup

`ConnectorRevit/Entry/App.cs` shows Connector and Converter **versions and build timestamps** separately on startup (they are built and deployed separately — a stale converter is otherwise invisible). Converter path is resolved under `%APPDATA%\Speckle\Kits\Objects\`.

## CloudBridge Tab (ribbon layout)

The `CloudBridge` tab (created in `App.cs:InitializeUiPanel`) has two panels — 4 top-level ribbon items total:

- **`Send/Receive`** (formerly "CloudBridge"): **Connector** (formerly "CloudBridge" button; icon `ConstructobotLogo64.png`) opens the DUI2 dockable pane; **Scheduler** opens the scheduled-send window. The Help & Resources pulldown (Forum/Tutorials/Docs/Manager) is commented out (`/* ... */`) near the bottom of `InitializeUiPanel` — preserved for restoration.
- **`Clash Review`** (added in `App.cs:InitializeClashNavigatorPanel`): **Previous** (`Btn_Previous3_p.png`) and **Next** (`Btn_Next3_p.png`) buttons — implemented in `ConnectorRevit/Entry/ClashNavigatorCommands.cs`, cycle through clash-marker elements in the active document.

**Why the Connector icon was swapped:** the original `logo16.png` / `logo32.png` assets in `ConnectorRevit/ConnectorRevit/Assets/` are yellow-warning-triangle images (not the Speckle logo). `ConstructobotLogo64.png` replaces them for the Connector button; the original PNGs are still on disk but unreferenced.

**New PNG assets** in `ConnectorRevit/ConnectorRevit/Assets/` (embedded via `ConnectorRevit.projitems`): `ConstructobotLogo64.png`, `Btn_Previous3_p.png`, `Btn_Next3_p.png`. Revit ribbon wants 16×16 (`Image`) and 32×32 (`LargeImage`) — single-size source files are reused at both slots and Revit downscales. Replace with crisp 16+32 variants if icons look blurry at compact ribbon width.

**Clash Navigator filter:** `BuiltInParameter.ALL_MODEL_MARK` starting with `"ClashSphere_"`. Unity writes this on each truss-clash sphere via `WallStudFixer.cs` (static counter `clashSphereCounter`, reset at the top of `NewEngine.FrameWalls`). Spheres ship to Revit exclusively on the `issues` branch — see `SpeckleHandler.cs:2119-2179`.

**Clash Navigator navigation:** `UIDocument.ShowElements(id)` + `Selection.SetElementIds({id})`. The spheres' per-instance description is written to `Comments`, so once selected it appears in Revit's Properties panel without extra UI.

**Tab sharing:** `CreateRibbonTab("CloudBridge")` in `InitializeClashNavigatorPanel` is wrapped in try/catch (ArgumentException) — expected to throw on every run because `InitializeUiPanel` has already created the tab. The catch is purely defensive; the panel is still added via `CreateRibbonPanel("CloudBridge", "Clash Review")`.

**Caveat:** the clash-sphere prefab's `speckle_type` is currently `Objects.BuiltElements.Level:...RevitLevel` (legacy value from whatever element the prefab was copied from). If spheres arrive in Revit as Levels they may not be selectable in 3D views — verify by receiving the `issues` branch and confirming they render as a selectable 3D element. Fix by updating the prefab's SpeckleProperties speckle_type to something geometric (e.g. `Objects.BuiltElements.Revit.DirectShape`) if needed.

## Relationship to cloudfab-unity

This repo provides the Speckle SDK that cloudfab-unity depends on. The Unity project lives at `C:\Users\RAMBAGE\git\cloudfab-unity`. The `Objects.dll` built here defines the data models (including `RevitWorkPlaneFamilyInstance`) that both Unity and Revit use to serialize/deserialize clip connectors.
