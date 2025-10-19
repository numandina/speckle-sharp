using System;
using System.IO;
using System.Text;
using System.Threading;

internal static class FileLogger
{
  private static readonly object _gate = new object();
  private static string _logPath = MakeDefaultPath();

  private static string MakeDefaultPath()
  {
    var root = Path.Combine(
      Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
      "Speckle", "Logs", "Revit-WP", DateTime.Now.ToString("yyyyMMdd-HHmmss")
    );
    Directory.CreateDirectory(root);
    return Path.Combine(root, "log.txt");
  }

  public static void SetPath(string path)
  {
    lock (_gate)
    {
      Directory.CreateDirectory(Path.GetDirectoryName(path)!);
      _logPath = path;
    }
  }

  public static void Log(string msg)
  {
    var line = $"[{DateTime.Now:HH:mm:ss.fff}] {msg}{Environment.NewLine}";
    lock (_gate)
    {
      // Allow other processes to read while we write
      using var fs = new FileStream(_logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
      using var sw = new StreamWriter(fs, Encoding.UTF8);
      sw.Write(line);
    }
  }

  public static void Log(Exception ex, string? prefix = null)
    => Log($"{prefix ?? "EX"}: {ex.GetType().Name} :: {ex.Message}\n{ex.StackTrace}");
}
