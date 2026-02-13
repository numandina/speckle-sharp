using Autodesk.Revit.DB;

namespace RevitSharedResources.Helpers.Extensions;

public static class ElementIdExtensions
{
#if REVIT2026
  public static int GetIntegerValue(this ElementId id) => (int)id.Value;
#else
  public static int GetIntegerValue(this ElementId id) => id.IntegerValue;
#endif
}
