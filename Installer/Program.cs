using System;
using System.IO;
using System.Linq;
using System.Reflection;

namespace CloudFabRevitInstaller;

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
      Console.WriteLine("  If Revit is running, close it and try again.");
      Console.WriteLine("  If the problem persists, contact support.");
      Console.WriteLine();
      WaitForKey();
      return 1;
    }
  }

  static int Run()
  {
    Console.WriteLine();
    Console.WriteLine("  CloudBridge Installer");
    Console.WriteLine("  =====================================");
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine("  IMPORTANT: Please close all Revit instances before proceeding.");
    Console.ResetColor();
    Console.WriteLine();

    string exeDir = AppContext.BaseDirectory;
    string appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);

    // Detect mode: multi-version (subdirectories named 20XX) or single-version (baked-in metadata)
    string[] versionDirs = Directory.GetDirectories(exeDir)
      .Select(Path.GetFileName)
      .Where(d => d.Length == 4 && d.StartsWith("20") && int.TryParse(d, out _))
      .OrderBy(d => d)
      .ToArray();

    if (versionDirs.Length > 0)
    {
      return InstallMultiple(exeDir, appData, versionDirs);
    }

    // Fallback: single-version mode (legacy layout with Connector/ and Kit/ next to exe)
    string version = Assembly.GetExecutingAssembly()
      .GetCustomAttributes<AssemblyMetadataAttribute>()
      .FirstOrDefault(a => a.Key == "RevitVersion")?.Value;

    if (version == null)
    {
      Error("Installation folders are missing.");
      Console.WriteLine("  Please extract the entire ZIP archive before running the installer.");
      Console.WriteLine();
      WaitForKey();
      return 1;
    }

    Console.WriteLine($"  Target Revit version: {version}");
    Console.WriteLine();

    bool ok = InstallVersion(exeDir, appData, version);

    Console.WriteLine();
    if (ok)
    {
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("  Installation complete!");
      Console.ResetColor();
      Console.WriteLine();
      Console.WriteLine($"  Connector: {Path.Combine(appData, "Autodesk", "Revit", "Addins", version)}");
      Console.WriteLine($"  Kit:       {Path.Combine(appData, "Speckle", "Kits", "Objects")}");
      Console.WriteLine();
      Console.WriteLine("  Restart Revit to load the connector.");
    }
    else
    {
      Console.ForegroundColor = ConsoleColor.Red;
      Console.WriteLine("  Installation failed. See errors above.");
      Console.ResetColor();
    }

    Console.WriteLine();
    WaitForKey();
    return ok ? 0 : 1;
  }

  static int InstallMultiple(string exeDir, string appData, string[] versions)
  {
    Console.WriteLine($"  Installing for Revit versions: {string.Join(", ", versions)}");
    Console.WriteLine();

    int installed = 0;
    int failed = 0;

    foreach (string version in versions)
    {
      string versionDir = Path.Combine(exeDir, version);
      Console.WriteLine($"  -- Revit {version} ------------------------------------------------");

      try
      {
        if (InstallVersion(versionDir, appData, version))
        {
          installed++;
        }
        else
        {
          failed++;
          Warn($"Revit {version} installation had errors (see above).");
        }
      }
      catch (IOException ex)
      {
        failed++;
        Error($"Revit {version}: {ex.Message}");
        Console.WriteLine("  Files may be locked. Make sure Revit is closed.");
      }
      catch (UnauthorizedAccessException ex)
      {
        failed++;
        Error($"Revit {version}: {ex.Message}");
        Console.WriteLine("  Access denied. Make sure Revit is closed.");
      }

      Console.WriteLine();
    }

    // ── Summary ───────────────────────────────────────────────────
    Console.WriteLine("  -- Summary --------------------------------------------------");
    Console.WriteLine();

    if (installed > 0)
    {
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"  Successfully installed for {installed} Revit version(s).");
      Console.ResetColor();
    }

    if (failed > 0)
    {
      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine($"  {failed} version(s) had errors. Close Revit and try again.");
      Console.ResetColor();
    }

    Console.WriteLine();
    Console.WriteLine("  Restart Revit to load the connector.");
    Console.WriteLine();

    WaitForKey();
    return failed > 0 ? 1 : 0;
  }

  static bool InstallVersion(string baseDir, string appData, string version)
  {
    bool success = true;

    // ── Part 1: Connector files ──────────────────────────────────
    string connectorSrc = Path.Combine(baseDir, "Connector");
    if (!Directory.Exists(connectorSrc))
    {
      Error($"Connector\\ folder not found for Revit {version}.");
      return false;
    }

    string addinsDir = Path.Combine(appData, "Autodesk", "Revit", "Addins", version);
    Directory.CreateDirectory(addinsDir);

    // Copy .addin manifest
    string addinSrc = Path.Combine(connectorSrc, "CloudBridge.addin");
    if (File.Exists(addinSrc))
    {
      File.Copy(addinSrc, Path.Combine(addinsDir, "CloudBridge.addin"), overwrite: true);
      Console.WriteLine($"  [OK] CloudBridge.addin -> Addins\\{version}\\");
    }
    else
    {
      Warn("CloudBridge.addin not found in Connector\\");
    }

    // Copy CloudBridge folder (all connector DLLs)
    // Clean destination first to remove stale DLLs from previous installs
    string cloudBridgeSrc = Path.Combine(connectorSrc, "CloudBridge");
    if (Directory.Exists(cloudBridgeSrc))
    {
      string cloudBridgeDest = Path.Combine(addinsDir, "CloudBridge");
      if (Directory.Exists(cloudBridgeDest))
      {
        // Safety: only clean if this looks like our install (contains our main DLL)
        bool isOurDir = File.Exists(Path.Combine(cloudBridgeDest, "CloudBridgeConnectorRevit.dll"))
                     || File.Exists(Path.Combine(cloudBridgeDest, "DesktopUI2.dll"));
        if (isOurDir)
        {
          // Delete only files in the directory (not subdirectories we don't own)
          foreach (string file in Directory.GetFiles(cloudBridgeDest))
            File.Delete(file);
        }
      }
      int copied = CopyDirectory(cloudBridgeSrc, cloudBridgeDest);
      Console.WriteLine($"  [OK] CloudBridge\\ ({copied} files) -> Addins\\{version}\\CloudBridge\\");
    }
    else
    {
      Error($"Connector\\CloudBridge\\ folder not found for Revit {version}.");
      success = false;
    }

    // ── Part 2: Kit files (Objects DLLs + templates) ─────────────
    string kitSrc = Path.Combine(baseDir, "Kit");
    if (Directory.Exists(kitSrc))
    {
      string kitDest = Path.Combine(appData, "Speckle", "Kits", "Objects");
      Directory.CreateDirectory(kitDest);
      int copied = CopyDirectory(kitSrc, kitDest);
      Console.WriteLine($"  [OK] Kit ({copied} files) -> Speckle\\Kits\\Objects\\");
    }
    else
    {
      Warn("No Kit\\ folder found — skipping Objects kit files.");
    }

    if (success)
    {
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"  Revit {version} — installed successfully.");
      Console.ResetColor();
    }

    return success;
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
