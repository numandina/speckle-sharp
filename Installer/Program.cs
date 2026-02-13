using System;
using System.IO;
using System.Linq;
using System.Reflection;

namespace CloudFabRevitInstaller;

class Program
{
  static int Main()
  {
    string version = Assembly.GetExecutingAssembly()
      .GetCustomAttributes<AssemblyMetadataAttribute>()
      .First(a => a.Key == "RevitVersion").Value;

    Console.WriteLine();
    Console.WriteLine("  CloudFab Speckle Connector Installer");
    Console.WriteLine("  =====================================");
    Console.WriteLine();
    Console.WriteLine($"  Target Revit version: {version}");
    Console.WriteLine();

    string exeDir = AppContext.BaseDirectory;
    string appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);

    // ── Part 1: Connector files ──────────────────────────────────
    string connectorSrc = Path.Combine(exeDir, "Connector");
    if (!Directory.Exists(connectorSrc))
    {
      Error("Connector\\ folder not found next to installer.");
      return 1;
    }

    string addinsDir = Path.Combine(appData, "Autodesk", "Revit", "Addins", version);
    Directory.CreateDirectory(addinsDir);

    // Copy .addin manifest
    string addinSrc = Path.Combine(connectorSrc, "SpeckleRevit2.addin");
    if (File.Exists(addinSrc))
    {
      File.Copy(addinSrc, Path.Combine(addinsDir, "SpeckleRevit2.addin"), overwrite: true);
      Console.WriteLine($"  [OK] SpeckleRevit2.addin -> Addins\\{version}\\");
    }
    else
    {
      Warn("SpeckleRevit2.addin not found in Connector\\");
    }

    // Copy SpeckleRevit2 folder (all connector DLLs)
    string speckleRevitSrc = Path.Combine(connectorSrc, "SpeckleRevit2");
    if (Directory.Exists(speckleRevitSrc))
    {
      string speckleRevitDest = Path.Combine(addinsDir, "SpeckleRevit2");
      int copied = CopyDirectory(speckleRevitSrc, speckleRevitDest);
      Console.WriteLine($"  [OK] SpeckleRevit2\\ ({copied} files) -> Addins\\{version}\\SpeckleRevit2\\");
    }
    else
    {
      Error("Connector\\SpeckleRevit2\\ folder not found.");
      return 1;
    }

    // ── Part 2: Kit files (Objects DLLs + templates) ─────────────
    string kitSrc = Path.Combine(exeDir, "Kit");
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

    // ── Done ─────────────────────────────────────────────────────
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine("  Installation complete!");
    Console.ResetColor();
    Console.WriteLine();
    Console.WriteLine($"  Connector: {addinsDir}");
    if (Directory.Exists(kitSrc))
      Console.WriteLine($"  Kit:       {Path.Combine(appData, "Speckle", "Kits", "Objects")}");
    Console.WriteLine();
    Console.WriteLine("  Restart Revit to load the connector.");
    Console.WriteLine();

    WaitForKey();
    return 0;
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
    WaitForKey();
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
