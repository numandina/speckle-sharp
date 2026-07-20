using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Autodesk.Revit.DB;
using Autodesk.Revit.DB.Structure;
using Objects.BuiltElements.Revit;
using RevitSharedResources.Helpers.Extensions;
using Speckle.Core.Models;
using Column = Objects.BuiltElements.Column;
using DB = Autodesk.Revit.DB;
using Line = Objects.Geometry.Line;
using Point = Objects.Geometry.Point;

namespace Objects.Converter.Revit;

public partial class ConverterRevit
{
  public ApplicationObject ColumnToNative(Column speckleColumn)
  {
    var docObj = GetExistingElementByApplicationId(speckleColumn.applicationId);
    var appObj = new ApplicationObject(speckleColumn.id, speckleColumn.speckle_type)
    {
      applicationId = speckleColumn.applicationId
    };

    // skip if element already exists in doc & receive mode is set to ignore
    if (IsIgnore(docObj, appObj))
    {
      return appObj;
    }

    if (speckleColumn.baseLine == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed, logItem: "Only line based Beams are currently supported.");
      return appObj;
    }

    var familySymbol = GetElementType<FamilySymbol>(speckleColumn, appObj, out bool isExactMatch);
    familySymbol = ResolveAtrSymbolOverride(speckleColumn, familySymbol, appObj, ref isExactMatch);
    if (familySymbol == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed);
      return appObj;
    }

    var baseLine = CurveToNative(speckleColumn.baseLine).get_Item(0);

    // If the start point elevation is higher than the end point elevation, reverse the line.
    if (baseLine.GetEndPoint(0).Z > baseLine.GetEndPoint(1).Z)
    {
      baseLine = DB.Line.CreateBound(baseLine.GetEndPoint(1), baseLine.GetEndPoint(0));
    }

    DB.FamilyInstance revitColumn = null;
    //var structuralType = StructuralType.Column;
    var isLineBased = true;

    var levelState = ApplicationObject.State.Unknown;
    double baseOffset = 0.0;
    DB.Level level =
      (speckleColumn.level != null)
        ? ConvertLevelToRevit(speckleColumn.level, out levelState)
        : ConvertLevelToRevit(baseLine, out levelState, out baseOffset);

    var speckleRevitColumn = speckleColumn as RevitColumn;

    double topOffset = 0.0;
    DB.Level topLevel = null;
    if (speckleRevitColumn != null)
    {
      topLevel = ConvertLevelToRevit(speckleRevitColumn.topLevel, out levelState);
      //structuralType = speckleRevitColumn.structural ? StructuralType.Column : StructuralType.NonStructural;
      //non slanted columns are point based
      isLineBased = speckleRevitColumn.isSlanted;
    }

    if (topLevel == null)
    {
      topLevel = ConvertLevelToRevit(baseLine.GetEndPoint(1), out levelState, out topOffset);
    }

    //try update existing

    bool isUpdate = false;
    if (docObj != null)
    {
      try
      {
        var revitType = Doc.GetElement(docObj.GetTypeId()) as ElementType;

        // if family changed, tough luck. delete and let us create a new one.
        if (familySymbol.FamilyName != revitType.FamilyName)
        {
          Doc.Delete(docObj.Id);
        }
        else
        {
          revitColumn = (DB.FamilyInstance)docObj;
          switch (revitColumn.Location)
          {
            case LocationCurve crv:
              crv.Curve = baseLine;
              break;
            case LocationPoint pt:
              pt.Point = baseLine.GetEndPoint(0);
              break;
          }

          // check for a type change
          if (isExactMatch && revitType.Id.GetIntegerValue() != familySymbol.Id.GetIntegerValue())
          {
            revitColumn.ChangeTypeId(familySymbol.Id);
          }
        }
        isUpdate = true;
      }
      catch (Autodesk.Revit.Exceptions.ApplicationException)
      {
        //something went wrong, re-create it
        appObj.Update(logItem: "Unable to update element. Creating a new element instead");
      }
    }

    if (revitColumn == null && isLineBased)
    {
      revitColumn = Doc.Create.NewFamilyInstance(baseLine, familySymbol, level, StructuralType.Column);
      if (revitColumn.Symbol.Family.FamilyPlacementType == FamilyPlacementType.CurveDrivenStructural)
      {
        StructuralFramingUtils.DisallowJoinAtEnd(revitColumn, 0);
        StructuralFramingUtils.DisallowJoinAtEnd(revitColumn, 1);
      }
    }

    var start = baseLine.GetEndPoint(0);
    var end = baseLine.GetEndPoint(1);
    var basePoint = start.Z < end.Z ? start : end; // pick the lowest
    //try with a point based column
    if (speckleRevitColumn != null && revitColumn == null && !isLineBased)
    {
      revitColumn = Doc.Create.NewFamilyInstance(basePoint, familySymbol, level, StructuralType.NonStructural);
    }

    //rotate
    if (speckleRevitColumn != null && revitColumn != null)
    {
      var currentRotation = (revitColumn.Location as LocationPoint)?.Rotation;

      if (currentRotation != null && currentRotation != speckleRevitColumn.rotation)
      {
        var axis = DB.Line.CreateBound(new XYZ(basePoint.X, basePoint.Y, 0), new XYZ(basePoint.X, basePoint.Y, 10000));
        var s = (revitColumn.Location as LocationPoint).Rotate(
          axis,
          speckleRevitColumn.rotation - (double)currentRotation
        );
      }
    }

    if (revitColumn == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed, logItem: "revit column was null");
      return appObj;
    }

    TrySetParam(revitColumn, BuiltInParameter.FAMILY_BASE_LEVEL_PARAM, level);
    TrySetParam(revitColumn, BuiltInParameter.FAMILY_TOP_LEVEL_PARAM, topLevel);

    if (speckleRevitColumn != null)
    {
      if (speckleRevitColumn.handFlipped != revitColumn.HandFlipped)
      {
        revitColumn.flipHand();
      }

      if (speckleRevitColumn.facingFlipped != revitColumn.FacingFlipped)
      {
        revitColumn.flipFacing();
      }

      //don't change offset for slanted columns, it's automatic
      if (!isLineBased)
      {
        SetOffsets(
          revitColumn,
          level,
          topLevel,
          ScaleToNative(speckleRevitColumn.baseOffset, speckleRevitColumn.units),
          ScaleToNative(speckleRevitColumn.topOffset, speckleRevitColumn.units)
        );
      }

      SetInstanceParameters(revitColumn, speckleRevitColumn);
    }
    else
    {
      // this case is always line based, don't change offset for line based columns, it's automatic
    }

    var state = isUpdate ? ApplicationObject.State.Updated : ApplicationObject.State.Created;
    appObj.Update(status: state, createdId: revitColumn.UniqueId, convertedItem: revitColumn);
    // TODO: nested elements.
    //appObj = SetHostedElements(speckleColumn, revitColumn, appObj);
    return appObj;
  }

  /// <summary>
  /// Some families eg columns, need offsets to be set in a specific way. This tries to cover that.
  /// </summary>
  /// <param name="speckleElement"></param>
  /// <param name="familyInstance"></param>
  private void SetOffsets(
    DB.FamilyInstance familyInstance,
    Level level,
    Level topLevel,
    double baseOffset,
    double topOffset
  )
  {
    var topOffsetParam = familyInstance.get_Parameter(BuiltInParameter.FAMILY_TOP_LEVEL_OFFSET_PARAM);
    var baseOffsetParam = familyInstance.get_Parameter(BuiltInParameter.FAMILY_BASE_LEVEL_OFFSET_PARAM);
    var baseLevelParam = familyInstance.get_Parameter(BuiltInParameter.FAMILY_BASE_LEVEL_PARAM);
    var topLevelParam = familyInstance.get_Parameter(BuiltInParameter.FAMILY_TOP_LEVEL_PARAM);

    if (topLevelParam == null || baseLevelParam == null || baseOffsetParam == null || topOffsetParam == null)
    {
      return;
    }

    // the column length cannot be 0 for even an instance or Revit will throw a fit.
    // Make sure that setting the offset on one side of the column before setting the
    // other side doesn't leave the length of the column as approximately 0
    var colHeightAfterBaseOffset = level.Elevation + baseOffset - topLevel.Elevation;
    var colHeightAfterTopOffset = topLevel.Elevation + topOffset - level.Elevation;

    if (Math.Abs(colHeightAfterBaseOffset) > TOLERANCE)
    {
      baseOffsetParam.Set(baseOffset);
      topOffsetParam.Set(topOffset);
    }
    else if (Math.Abs(colHeightAfterTopOffset) > TOLERANCE)
    {
      topOffsetParam.Set(topOffset);
      baseOffsetParam.Set(baseOffset);
    }
    else
    {
      baseOffsetParam.Set(baseOffset / 2); // temporarily set this value to something else so the sides of the column can switch places
      topOffsetParam.Set(topOffset);
      baseOffsetParam.Set(baseOffset);
    }
  }

  public Base ColumnToSpeckle(DB.FamilyInstance revitColumn, out List<string> notes)
  {
    notes = new List<string>();
    var symbol = (FamilySymbol)revitColumn.Document.GetElement(revitColumn.GetTypeId());

    RevitLevel level = ConvertAndCacheLevel(revitColumn, BuiltInParameter.FAMILY_BASE_LEVEL_PARAM);
    RevitLevel topLevel = ConvertAndCacheLevel(revitColumn, BuiltInParameter.FAMILY_TOP_LEVEL_PARAM);
    double baseOffset = GetParamValue<double>(revitColumn, BuiltInParameter.FAMILY_BASE_LEVEL_OFFSET_PARAM);
    double topOffset = GetParamValue<double>(revitColumn, BuiltInParameter.FAMILY_TOP_LEVEL_OFFSET_PARAM);

    //geometry
    var baseGeometry = LocationToSpeckle(revitColumn);
    var baseLine = baseGeometry as ICurve;

    if (baseLine == null && baseGeometry is Point basePoint)
    //make line from point and height
    {
      if (symbol.Family.FamilyPlacementType is FamilyPlacementType.OneLevelBased or FamilyPlacementType.WorkPlaneBased)
      {
        return RevitInstanceToSpeckle(revitColumn, out notes, null);
      }

      var elevation = topLevel.elevation;
      baseLine = new Line(
        basePoint,
        new Point(basePoint.x, basePoint.y, elevation + topOffset, ModelUnits),
        ModelUnits
      );
    }

    if (baseLine == null)
    {
      return RevitElementToSpeckle(revitColumn, out notes);
    }

    double rotation = revitColumn.Location is LocationPoint location ? location.Rotation : 0;

    var speckleColumn = new RevitColumn(
      symbol.FamilyName,
      revitColumn.Document.GetElement(revitColumn.GetTypeId()).Name,
      baseLine, //all speckle columns should be line based
      level,
      topLevel,
      ModelUnits,
      revitColumn.Id.ToString(),
      baseOffset,
      topOffset,
      revitColumn.FacingFlipped,
      revitColumn.HandFlipped,
      revitColumn.IsSlantedColumn,
      rotation,
      GetElementDisplayValue(revitColumn)
    //structural: revitColumn.StructuralType == StructuralType.Column;
    );

    GetAllRevitParamsAndIds(
      speckleColumn,
      revitColumn,
      new List<string>
      {
        "FAMILY_BASE_LEVEL_PARAM",
        "FAMILY_TOP_LEVEL_PARAM",
        "FAMILY_BASE_LEVEL_OFFSET_PARAM",
        "FAMILY_TOP_LEVEL_OFFSET_PARAM",
        "SCHEDULE_BASE_LEVEL_OFFSET_PARAM",
        "SCHEDULE_TOP_LEVEL_OFFSET_PARAM"
      }
    );

    return speckleColumn;
  }

  /// <summary>
  /// CloudFab threaded-rod shear walls: rods and couplers arrive as RevitColumn with
  /// generic geometry plus "rodDiameter" / "couplerBottomDia" / "couplerTopDia" /
  /// "couplerTypeName" properties (inches). Their families are Generic Models, which the
  /// category-filtered column type lookup can never see, so this searches the whole
  /// document. Rods map to a type by its Diameter parameter (created if missing);
  /// couplers map to a type by name (e.g. 0.375" TO 0.75" COUPLER).
  /// </summary>
  private FamilySymbol ResolveAtrSymbolOverride(
    Column speckleColumn,
    FamilySymbol resolved,
    ApplicationObject appObj,
    ref bool isExactMatch
  )
  {
    double rodDiaInches = GetAtrDoubleProperty(speckleColumn, "rodDiameter");
    double couplerBottom = GetAtrDoubleProperty(speckleColumn, "couplerBottomDia");
    var couplerTypeName = speckleColumn["couplerTypeName"] as string;

    if (rodDiaInches <= 0 && couplerBottom <= 0 && string.IsNullOrWhiteSpace(couplerTypeName))
    {
      return resolved;
    }

    var allSymbols = new FilteredElementCollector(Doc).OfClass(typeof(FamilySymbol)).Cast<FamilySymbol>().ToList();
    var familyName = GetElementFamily(speckleColumn);

    if (couplerBottom > 0 || !string.IsNullOrWhiteSpace(couplerTypeName))
    {
      if (string.IsNullOrWhiteSpace(couplerTypeName))
      {
        couplerTypeName = string.Format(
          CultureInfo.InvariantCulture,
          "{0:0.###}\" TO {1:0.###}\" COUPLER",
          couplerBottom,
          GetAtrDoubleProperty(speckleColumn, "couplerTopDia")
        );
      }

      var couplerSymbol =
        allSymbols.FirstOrDefault(s =>
          AtrNameEquals(s.FamilyName, familyName) && AtrNameEquals(s.Name, couplerTypeName)
        ) ?? allSymbols.FirstOrDefault(s => AtrNameEquals(s.Name, couplerTypeName));

      if (couplerSymbol != null)
      {
        isExactMatch = true;
        return ActivatedSymbol(couplerSymbol);
      }

      appObj.Update(
        logItem: $"Coupler type '{couplerTypeName}' not found in project — rename the placed instance's type manually"
      );
      return resolved;
    }

    var rodFamilySymbols = allSymbols.Where(s => AtrNameEquals(s.FamilyName, familyName)).ToList();
    if (rodFamilySymbols.Count == 0 && resolved != null)
    {
      rodFamilySymbols = allSymbols.Where(s => s.Family.Id == resolved.Family.Id).ToList();
    }

    const double toleranceInches = 1.0 / 32.0;
    foreach (var symbol in rodFamilySymbols)
    {
      var dia = symbol.LookupParameter("Diameter");
      if (
        dia != null
        && dia.StorageType == StorageType.Double
        && Math.Abs(dia.AsDouble() * 12.0 - rodDiaInches) < toleranceInches
      )
      {
        isExactMatch = true;
        return ActivatedSymbol(symbol);
      }
    }

    var template = rodFamilySymbols.FirstOrDefault(s =>
    {
      var dia = s.LookupParameter("Diameter");
      return dia != null && !dia.IsReadOnly && dia.StorageType == StorageType.Double;
    });

    if (template != null)
    {
      var newTypeName = FormatAtrInchesFraction(rodDiaInches) + "\" ATR";
      // a type with the target name but a different Diameter may already exist —
      // Duplicate throws on the name collision, so fall through to a suffixed name
      foreach (var candidate in new[] { newTypeName, newTypeName + " (CloudFab)" })
      {
        try
        {
          var duplicated = (FamilySymbol)template.Duplicate(candidate);
          duplicated.LookupParameter("Diameter").Set(rodDiaInches / 12.0);
          appObj.Update(logItem: $"Created rod type '{candidate}' with Diameter {rodDiaInches}\"");
          isExactMatch = true;
          return ActivatedSymbol(duplicated);
        }
        catch (Autodesk.Revit.Exceptions.ApplicationException) { }
      }
    }

    appObj.Update(
      logItem: $"No type with Diameter {rodDiaInches}\" found in family '{familyName ?? "Unknown"}' and none could be created"
    );
    return rodFamilySymbols.Count > 0 ? ActivatedSymbol(rodFamilySymbols[0]) : resolved;
  }

  private static FamilySymbol ActivatedSymbol(FamilySymbol symbol)
  {
    if (!symbol.IsActive)
    {
      symbol.Activate();
    }
    return symbol;
  }

  private static bool AtrNameEquals(string a, string b)
  {
    return !string.IsNullOrWhiteSpace(a)
      && !string.IsNullOrWhiteSpace(b)
      && string.Equals(a.Trim(), b.Trim(), StringComparison.OrdinalIgnoreCase);
  }

  private static double GetAtrDoubleProperty(Base element, string key)
  {
    var value = element[key];
    if (value == null)
    {
      return 0;
    }
    try
    {
      return Convert.ToDouble(value, CultureInfo.InvariantCulture);
    }
    catch (SystemException)
    {
      return 0;
    }
  }

  private static string FormatAtrInchesFraction(double inches)
  {
    int whole = (int)Math.Floor(inches + 1e-9);
    int sixteenths = (int)Math.Round((inches - whole) * 16.0);
    if (sixteenths == 16)
    {
      whole++;
      sixteenths = 0;
    }
    if (sixteenths == 0)
    {
      return whole.ToString(CultureInfo.InvariantCulture);
    }
    int numerator = sixteenths;
    int denominator = 16;
    while (numerator % 2 == 0)
    {
      numerator /= 2;
      denominator /= 2;
    }
    var fraction = numerator + "/" + denominator;
    return whole > 0 ? whole + " " + fraction : fraction;
  }
}
