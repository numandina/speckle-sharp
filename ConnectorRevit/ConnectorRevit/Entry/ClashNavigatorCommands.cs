using System.Collections.Generic;
using System.Linq;
using Autodesk.Revit.Attributes;
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;

namespace Speckle.ConnectorRevit.Entry;

internal static class ClashNavigatorState
{
  internal const string MarkPrefix = "ClashSphere_";
  internal static int currentIndex = -1;

  internal static Result Navigate(ExternalCommandData cd, int delta, ref string message)
  {
    var uidoc = cd.Application.ActiveUIDocument;
    if (uidoc == null)
    {
      message = "No active document.";
      return Result.Cancelled;
    }

    var doc = uidoc.Document;

    var spheres = new FilteredElementCollector(doc)
      .WhereElementIsNotElementType()
      .Where(e =>
      {
        var p = e.get_Parameter(BuiltInParameter.ALL_MODEL_MARK);
        var s = p?.AsString();
        return !string.IsNullOrEmpty(s) && s.StartsWith(MarkPrefix);
      })
      .OrderBy(e =>
      {
        var s = e.get_Parameter(BuiltInParameter.ALL_MODEL_MARK).AsString();
        return int.TryParse(s.Substring(MarkPrefix.Length), out var n) ? n : 0;
      })
      .ToList();

    if (spheres.Count == 0)
    {
      TaskDialog.Show("Clash Navigator", "No clash spheres found in the current document.");
      return Result.Cancelled;
    }

    currentIndex = (currentIndex + delta) % spheres.Count;
    if (currentIndex < 0)
      currentIndex += spheres.Count;

    var target = spheres[currentIndex];
    uidoc.ShowElements(target.Id);
    uidoc.Selection.SetElementIds(new List<ElementId> { target.Id });

    return Result.Succeeded;
  }
}

[Transaction(TransactionMode.ReadOnly)]
public class NextClashCommand : IExternalCommand
{
  public Result Execute(ExternalCommandData commandData, ref string message, ElementSet elements)
    => ClashNavigatorState.Navigate(commandData, +1, ref message);
}

[Transaction(TransactionMode.ReadOnly)]
public class PreviousClashCommand : IExternalCommand
{
  public Result Execute(ExternalCommandData commandData, ref string message, ElementSet elements)
    => ClashNavigatorState.Navigate(commandData, -1, ref message);
}
