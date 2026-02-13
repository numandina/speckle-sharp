using Autodesk.Revit.DB;
using RevitSharedResources.Helpers.Extensions;

namespace ConverterRevitShared.Extensions;

internal static class CategoryExtensions
{
  public static bool EqualsBuiltInCategory(this Category category, BuiltInCategory builtInCategory)
  {
# if REVIT2020 || REVIT2021 || REVIT2022
    return category.Id.GetIntegerValue() == (int)builtInCategory;
#else
    return category.BuiltInCategory == builtInCategory;
#endif
  }
}
