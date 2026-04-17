using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace CloudFabInstaller;

class Program
{
  static int Main()
  {
    try
    {
      return Run();
    }
    catch (Exception ex)
    {
      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Red;
      Console.WriteLine($"  FATAL ERROR: {ex.Message}");
      Console.ResetColor();
      Console.WriteLine();
      Console.WriteLine("  Close all Autodesk, Rhino, and Dynamo applications and try again.");
      Console.WriteLine();
      WaitForKey();
      return 1;
    }
  }

  static int Run()
  {
    string appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
    string exeDir = AppContext.BaseDirectory;

    Console.WriteLine();
    Console.WriteLine("  CloudFab Connector Installer");
    Console.WriteLine("  =====================================");
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine("  IMPORTANT: Close all Revit, Rhino, AutoCAD, Navisworks,");
    Console.WriteLine("  and Dynamo instances before proceeding.");
    Console.ResetColor();
    Console.WriteLine();

    // ── Detect connectors ────────────────────────────────────────
    var connectors = DetectConnectors(exeDir);
    bool hasKit = Directory.Exists(Path.Combine(exeDir, "Kit"));

    if (connectors.Count == 0)
    {
      Error("No connector folders found next to the installer.");
      Console.WriteLine("  Please extract the entire ZIP archive before running.");
      Console.WriteLine();
      WaitForKey();
      return 1;
    }

    // ── Display what was found ───────────────────────────────────
    Console.WriteLine("  Detected connectors:");
    foreach (var group in connectors.GroupBy(c => c.Type))
    {
      string versions = string.Join(", ", group.Select(c => c.Version).Where(v => v != ""));
      Console.WriteLine(versions.Length > 0
        ? $"    {group.Key}: {versions}"
        : $"    {group.Key}");
    }
    if (hasKit)
      Console.WriteLine("    Objects Kit (shared)");
    Console.WriteLine();

    // ── Install ──────────────────────────────────────────────────
    int installed = 0;
    int failed = 0;

    foreach (var conn in connectors)
    {
      string label = conn.Version != "" ? $"{conn.Type} {conn.Version}" : conn.Type;
      Console.WriteLine($"  -- {label} ------------------------------------------------");

      try
      {
        bool ok = conn.Type switch
        {
          "Revit"       => InstallRevit(conn.Dir, appData, conn.Version),
          "AutoCAD"     => InstallAutoCAD(conn.Dir, appData, conn.Version),
          "Civil3D"     => InstallCivil3D(conn.Dir, appData, conn.Version),
          "Rhino"       => InstallRhino(conn.Dir, appData, conn.Version),
          "Grasshopper" => InstallGrasshopper(conn.Dir, appData, conn.Version),
          "Navisworks"  => InstallNavisworks(conn.Dir, appData, conn.Version),
          "Dynamo"      => InstallDynamo(conn.Dir, appData),
          _ => false
        };

        if (ok) installed++;
        else failed++;
      }
      catch (IOException ex)
      {
        failed++;
        Error(ex.Message);
        Console.WriteLine("  Files may be locked — close the application and retry.");
      }
      catch (UnauthorizedAccessException ex)
      {
        failed++;
        Error(ex.Message);
        Console.WriteLine("  Access denied — close the application and retry.");
      }

      Console.WriteLine();
    }

    // ── Shared Kit ───────────────────────────────────────────────
    if (hasKit)
    {
      Console.WriteLine("  -- Objects Kit ------------------------------------------------");
      InstallKit(Path.Combine(exeDir, "Kit"), appData);
      Console.WriteLine();
    }

    // ── Summary ──────────────────────────────────────────────────
    Console.WriteLine("  -- Summary --------------------------------------------------");
    Console.WriteLine();

    if (installed > 0)
    {
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"  Successfully installed {installed} connector(s).");
      Console.ResetColor();
    }

    if (failed > 0)
    {
      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine($"  {failed} connector(s) had errors. Close applications and try again.");
      Console.ResetColor();
    }

    Console.WriteLine();
    Console.WriteLine("  Restart your applications to load the connectors.");
    Console.WriteLine();

    WaitForKey();
    return failed > 0 ? 1 : 0;
  }

  // ══ Detection ════════════════════════════════════════════════════

  record ConnectorInfo(string Type, string Version, string Dir);

  static List<ConnectorInfo> DetectConnectors(string exeDir)
  {
    var results = new List<ConnectorInfo>();
    string[] versionedTypes = { "Revit", "AutoCAD", "Civil3D", "Rhino", "Grasshopper", "Navisworks" };

    foreach (string type in versionedTypes)
    {
      string typeDir = Path.Combine(exeDir, type);
      if (!Directory.Exists(typeDir)) continue;

      var versions = Directory.GetDirectories(typeDir)
        .Select(Path.GetFileName)
        .Where(d => int.TryParse(d, out _))
        .OrderBy(d => d)
        .ToArray();

      foreach (string ver in versions)
        results.Add(new(type, ver, Path.Combine(typeDir, ver)));
    }

    // Dynamo (no version subfolders — single package)
    string dynamoDir = Path.Combine(exeDir, "Dynamo");
    if (Directory.Exists(dynamoDir))
      results.Add(new("Dynamo", "", dynamoDir));

    // Legacy: year folders directly next to exe → treat as Revit-only
    if (results.Count == 0)
    {
      var yearDirs = Directory.GetDirectories(exeDir)
        .Select(Path.GetFileName)
        .Where(d => d.Length == 4 && d.StartsWith("20") && int.TryParse(d, out _))
        .OrderBy(d => d)
        .ToArray();

      foreach (string year in yearDirs)
        results.Add(new("Revit", year, Path.Combine(exeDir, year)));
    }

    return results;
  }

  // ══ Revit ════════════════════════════════════════════════════════
  // .addin + CloudBridge/ → %AppData%\Autodesk\Revit\Addins\{year}\

  static bool InstallRevit(string srcDir, string appData, string version)
  {
    bool success = true;

    string connectorSrc = Path.Combine(srcDir, "Connector");
    if (!Directory.Exists(connectorSrc))
    {
      Error($"Connector\\ folder not found for Revit {version}.");
      return false;
    }

    string addinsDir = Path.Combine(appData, "Autodesk", "Revit", "Addins", version);
    Directory.CreateDirectory(addinsDir);

    // .addin manifest
    string addinSrc = Path.Combine(connectorSrc, "CloudBridge.addin");
    if (File.Exists(addinSrc))
    {
      File.Copy(addinSrc, Path.Combine(addinsDir, "CloudBridge.addin"), overwrite: true);
      Ok($"CloudBridge.addin -> Addins\\{version}\\");
    }
    else
    {
      Warn("CloudBridge.addin not found");
    }

    // CloudBridge DLL folder
    string cloudBridgeSrc = Path.Combine(connectorSrc, "CloudBridge");
    if (Directory.Exists(cloudBridgeSrc))
    {
      string dest = Path.Combine(addinsDir, "CloudBridge");
      CleanOurDirectory(dest, "CloudBridgeConnectorRevit.dll", "DesktopUI2.dll");
      int copied = CopyDirectory(cloudBridgeSrc, dest);
      Ok($"CloudBridge\\ ({copied} files) -> Addins\\{version}\\CloudBridge\\");
    }
    else
    {
      Error($"Connector\\CloudBridge\\ not found for Revit {version}.");
      success = false;
    }

    // Per-version Kit (if present alongside connector)
    string kitSrc = Path.Combine(srcDir, "Kit");
    if (Directory.Exists(kitSrc))
    {
      string kitDest = Path.Combine(appData, "Speckle", "Kits", "Objects");
      Directory.CreateDirectory(kitDest);
      int copied = CopyDirectory(kitSrc, kitDest);
      Ok($"Kit ({copied} files)");
    }

    if (success)
    {
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"  Revit {version} — installed.");
      Console.ResetColor();
    }

    return success;
  }

  // ══ AutoCAD ══════════════════════════════════════════════════════
  // DLLs → %AppData%\Autodesk\ApplicationPlugins\Speckle2AutoCAD{year}\

  static bool InstallAutoCAD(string srcDir, string appData, string version)
  {
    string dest = Path.Combine(appData, "Autodesk", "ApplicationPlugins", $"Speckle2AutoCAD{version}");
    CleanOurDirectory(dest, "SpeckleConnectorAutocad.dll");
    int copied = CopyDirectory(srcDir, dest);
    Ok($"{copied} files -> AutoCAD {version}");

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"  AutoCAD {version} — installed.");
    Console.ResetColor();
    return true;
  }

  // ══ Civil 3D ═════════════════════════════════════════════════════
  // DLLs → %AppData%\Autodesk\ApplicationPlugins\Speckle2Civil3D{year}\

  static bool InstallCivil3D(string srcDir, string appData, string version)
  {
    string dest = Path.Combine(appData, "Autodesk", "ApplicationPlugins", $"Speckle2Civil3D{version}");
    CleanOurDirectory(dest, "SpeckleConnectorCivil.dll");
    int copied = CopyDirectory(srcDir, dest);
    Ok($"{copied} files -> Civil 3D {version}");

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"  Civil 3D {version} — installed.");
    Console.ResetColor();
    return true;
  }

  // ══ Rhino ════════════════════════════════════════════════════════
  // .rhp + DLLs → %AppData%\McNeel\Rhinoceros\{ver}.0\Plug-ins\SpeckleConnectorRhino\

  static bool InstallRhino(string srcDir, string appData, string version)
  {
    string dest = Path.Combine(appData, "McNeel", "Rhinoceros", $"{version}.0",
                               "Plug-ins", "SpeckleConnectorRhino");
    CleanOurDirectory(dest, "SpeckleConnectorRhino.rhp");
    int copied = CopyDirectory(srcDir, dest);
    Ok($"{copied} files -> Rhino {version}");

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"  Rhino {version} — installed.");
    Console.ResetColor();
    return true;
  }

  // ══ Grasshopper ══════════════════════════════════════════════════
  // .gha + DLLs → %AppData%\Grasshopper\Libraries\SpeckleConnectorGrasshopper{ver}\
  // Versioned subfolder avoids conflicts when multiple Rhino versions coexist.

  static bool InstallGrasshopper(string srcDir, string appData, string version)
  {
    string dest = Path.Combine(appData, "Grasshopper", "Libraries",
                               $"SpeckleConnectorGrasshopper{version}");
    CleanOurDirectory(dest, "SpeckleConnectorGrasshopper.gha");
    int copied = CopyDirectory(srcDir, dest);
    Ok($"{copied} files -> Grasshopper {version}");

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"  Grasshopper {version} — installed.");
    Console.ResetColor();
    return true;
  }

  // ══ Navisworks ═══════════════════════════════════════════════════
  // Bundle → %AppData%\Autodesk\ApplicationPlugins\Speckle.ConnectorNavisworks.bundle\
  // Year maps to bundle version: 2024 → v21 (year - 2003)

  static bool InstallNavisworks(string srcDir, string appData, string version)
  {
    string bundleDir = Path.Combine(appData, "Autodesk", "ApplicationPlugins",
                                    "Speckle.ConnectorNavisworks.bundle");
    Directory.CreateDirectory(bundleDir);

    // PackageContents.xml lives in the Navisworks/ root (one level up from year folder)
    string packageXml = Path.Combine(Path.GetDirectoryName(srcDir)!, "PackageContents.xml");
    if (File.Exists(packageXml))
    {
      File.Copy(packageXml, Path.Combine(bundleDir, "PackageContents.xml"), overwrite: true);
      Ok("PackageContents.xml -> bundle root");
    }

    // Map year → bundle version
    int year = int.Parse(version);
    string bundleVer = $"v{year - 2003}";

    string dest = Path.Combine(bundleDir, "Contents", bundleVer);
    CleanOurDirectory(dest, "SpeckleConnectorNavisworks.dll");
    int copied = CopyDirectory(srcDir, dest);
    Ok($"{copied} files -> ...bundle\\Contents\\{bundleVer}\\");

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"  Navisworks {version} — installed.");
    Console.ResetColor();
    return true;
  }

  // ══ Dynamo ═══════════════════════════════════════════════════════
  // Package → %AppData%\Dynamo\Dynamo Revit\*\packages\SpeckleDynamo2\
  // Scans for all existing Dynamo installations and installs to each.

  static bool InstallDynamo(string srcDir, string appData)
  {
    string dynamoRoot = Path.Combine(appData, "Dynamo");
    if (!Directory.Exists(dynamoRoot))
    {
      Warn("Dynamo not found at %AppData%\\Dynamo\\ — skipping.");
      Warn("Install Dynamo first, then re-run this installer.");
      return true; // Not a failure — Dynamo just isn't installed
    }

    int locations = 0;

    // Scan Dynamo host directories (e.g. "Dynamo Core", "Dynamo Revit")
    foreach (string hostDir in Directory.GetDirectories(dynamoRoot))
    {
      foreach (string versionDir in Directory.GetDirectories(hostDir))
      {
        string packagesDir = Path.Combine(versionDir, "packages");
        if (!Directory.Exists(packagesDir)) continue;

        string dest = Path.Combine(packagesDir, "SpeckleDynamo2");
        int copied = CopyDirectory(srcDir, dest);
        string hostName = Path.GetFileName(hostDir);
        string ver = Path.GetFileName(versionDir);
        Ok($"{hostName} {ver} ({copied} files)");
        locations++;
      }
    }

    if (locations > 0)
    {
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"  Dynamo — installed to {locations} location(s).");
      Console.ResetColor();
    }
    else
    {
      Warn("No Dynamo package directories found.");
    }

    return true;
  }

  // ══ Shared Objects Kit ═══════════════════════════════════════════
  // Objects DLLs + templates → %AppData%\Speckle\Kits\Objects\

  static bool InstallKit(string srcDir, string appData)
  {
    string dest = Path.Combine(appData, "Speckle", "Kits", "Objects");
    Directory.CreateDirectory(dest);
    int copied = CopyDirectory(srcDir, dest);
    Ok($"{copied} files -> Objects Kit");

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine("  Objects Kit — installed.");
    Console.ResetColor();
    return true;
  }

  // ══ Utilities ════════════════════════════════════════════════════

  /// <summary>
  /// Delete files in a directory only if it looks like a previous installation of ours
  /// (contains at least one of the marker files). Prevents accidentally wiping unrelated folders.
  /// </summary>
  static void CleanOurDirectory(string dir, params string[] markerFiles)
  {
    if (!Directory.Exists(dir)) return;
    bool isOurs = markerFiles.Any(m => File.Exists(Path.Combine(dir, m)));
    if (!isOurs) return;

    foreach (string file in Directory.GetFiles(dir))
      File.Delete(file);
  }

  static int CopyDirectory(string src, string dest)
  {
    int count = 0;
    Directory.CreateDirectory(dest);

    foreach (string file in Directory.GetFiles(src))
    {
      File.Copy(file, Path.Combine(dest, Path.GetFileName(file)), overwrite: true);
      count++;
    }

    foreach (string dir in Directory.GetDirectories(src))
    {
      count += CopyDirectory(dir, Path.Combine(dest, Path.GetFileName(dir)));
    }

    return count;
  }

  static void Ok(string msg)
  {
    Console.Write("  [");
    Console.ForegroundColor = ConsoleColor.Green;
    Console.Write("OK");
    Console.ResetColor();
    Console.WriteLine($"] {msg}");
  }

  static void Error(string msg)
  {
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"  ERROR: {msg}");
    Console.ResetColor();
  }

  static void Warn(string msg)
  {
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"  [WARN] {msg}");
    Console.ResetColor();
  }

  static void WaitForKey()
  {
    Console.Write("  Press any key to exit...");
    Console.ReadKey(true);
    Console.WriteLine();
  }
}
