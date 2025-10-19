// File: DebugFileLogger.cs (any folder/namespace is fine)
using System;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;

internal static class Dbg
{
  private static string LogDir =>
    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Speckle", "Logs");

  private static string LogPath => Path.Combine(LogDir, "converterrevit_dbg.log");

  internal static void W(string msg)
  {
    try
    {
      Directory.CreateDirectory(LogDir);
      using var fs = new FileStream(LogPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
      using var sw = new StreamWriter(fs);
      sw.WriteLine($"{DateTime.Now:O} {msg}");
    }
    catch { /* never throw from logging */ }
  }

  // Optional: one-liner that includes the calling member name
  internal static void Here(string msg = "", [CallerMemberName] string member = "")
    => W($"[{member}] {msg}");
}

// Optional: proves the DLL loaded even before any constructor runs
internal static class ModuleProbe
{
  internal static void Init()
  {
    Dbg.W($"[ModuleInit] assembly={Assembly.GetExecutingAssembly().Location}");
  }
}
