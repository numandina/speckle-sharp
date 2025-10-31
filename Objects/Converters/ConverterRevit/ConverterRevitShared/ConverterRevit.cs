#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using Autodesk.Revit.DB;
using Autodesk.Revit.DB.Architecture;
using Objects.BuiltElements.Revit;
using Objects.GIS;
using Objects.Organization;
using Objects.Other;
using Objects.Structural.Properties.Profiles;
using RevitSharedResources.Helpers.Extensions;
using RevitSharedResources.Interfaces;
using RevitSharedResources.Models;
using Speckle.Core.Kits;
using Speckle.Core.Models;
using Speckle.Core.Models.Extensions;
using BE = Objects.BuiltElements;
using BER = Objects.BuiltElements.Revit;
using BERC = Objects.BuiltElements.Revit.Curve;
using DB = Autodesk.Revit.DB;
using STR = Objects.Structural;
using System.Linq;
using OG = Objects.Geometry;
using OR = Objects.Other.Revit;
using System.Collections.Generic;
using Objects.Geometry;
namespace Objects.Converter.Revit;
using OG = Objects.Geometry;
using System.Linq;
using DB = Autodesk.Revit.DB;
using OR = Objects.Other.Revit;

public partial class ConverterRevit : ISpeckleConverter
{
#if REVIT2025
  public static string RevitAppName = HostApplications.Revit.GetVersion(HostAppVersion.v2025);
#elif REVIT2024
  public static string RevitAppName = HostApplications.Revit.GetVersion(HostAppVersion.v2024);
#elif REVIT2023
  public static string RevitAppName = HostApplications.Revit.GetVersion(HostAppVersion.v2023);
#elif REVIT2022
  public static string RevitAppName = HostApplications.Revit.GetVersion(HostAppVersion.v2022);
#elif REVIT2021
  public static string RevitAppName = HostApplications.Revit.GetVersion(HostAppVersion.v2021);
#elif REVIT2020
  public static string RevitAppName = HostApplications.Revit.GetVersion(HostAppVersion.v2020);
#elif REVIT2019
  public static string RevitAppName = HostApplications.Revit.GetVersion(HostAppVersion.v2019);
#endif

  #region ISpeckleConverter props

  public string Description => "Default Speckle Kit for Revit";
  public string Name => nameof(ConverterRevit);
  public string Author => "Speckle";
  public string WebsiteOrEmail => "https://speckle.systems";

  public IEnumerable<string> GetServicedApplications() => new string[] { RevitAppName };

  #endregion ISpeckleConverter props

  private const double TOLERANCE = 0.0164042; // 5mm in ft

  public Document Doc { get; private set; }

  /// <summary>
  /// <para>To know which other objects are being converted, in order to sort relationships between them.
  /// For example, elements that have children use this to determine whether they should send their children out or not.</para>
  /// </summary>
  public Dictionary<string, ApplicationObject> ContextObjects { get; set; } =
    new Dictionary<string, ApplicationObject>();

  /// <summary>
  /// <para>To keep track of previously received objects from a given stream in here. If possible, conversions routines
  /// will edit an existing object, otherwise they will delete the old one and create the new one.</para>
  /// </summary>
  public IReceivedObjectIdMap<Base, Element> PreviouslyReceivedObjectIds { get; set; }

  /// <summary>
  /// Keeps track of the current host element that is creating any sub-objects it may have.
  /// </summary>
  public Element CurrentHostElement => RevitConverterState.Peek?.CurrentHostElement;

  /// <summary>
  /// Used when sending; keeps track of all the converted objects so far. Child elements first check in here if they should convert themselves again (they may have been converted as part of a parent's hosted elements).
  /// </summary>
  public ISet<string> ConvertedObjects { get; private set; } = new HashSet<string>();

  public ProgressReport Report { get; private set; } = new ProgressReport();

  public Dictionary<string, string> Settings { get; private set; } = new Dictionary<string, string>();

  public Dictionary<string, BE.Level> Levels { get; private set; } = new Dictionary<string, BE.Level>();

  public Dictionary<string, Phase> Phases { get; private set; } = new Dictionary<string, Phase>();

  /// <summary>
  /// Used to cache already converted family instance FamilyType deifnitions
  /// </summary>
  public Dictionary<string, Objects.BuiltElements.Revit.RevitSymbolElementType> Symbols { get; private set; } =
    new Dictionary<string, Objects.BuiltElements.Revit.RevitSymbolElementType>();

  public Dictionary<string, SectionProfile> SectionProfiles { get; private set; } =
    new Dictionary<string, SectionProfile>();

  public ReceiveMode ReceiveMode { get; set; }

  /// <summary>
  /// Contains all materials in the model
  /// </summary>
  public Dictionary<string, Objects.Other.Material> Materials { get; private set; } =
    new Dictionary<string, Objects.Other.Material>();
  private void DebugLog(string msg)
  {
    try { Report.Log(msg); } catch { /* ignore UI failures */ }
    try { FileLogger.Log(msg); } catch { /* ignore IO failures */ }
  }

  public ConverterRevit()
  {

    var ver = System.Reflection.Assembly.GetAssembly(typeof(ConverterRevit)).GetName().Version;
    DebugLog($"Using converter: {Name} v{ver}");

    // Optional: choose an explicit path instead of default
    // FileLogger.SetPath(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
    //   "Speckle","Logs","Revit-WP","latest.txt"));

    FileLogger.Log($"Converter boot. Objects asm: {typeof(Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance).Assembly.Location}");
    Report.Log($"Using converter: {Name} v{ver}");
  }

  private IRevitDocumentAggregateCache? revitDocumentAggregateCache;
  private IConvertedObjectsCache<Base, Element> receivedObjectsCache;
  private TransactionManager transactionManager;

  public void SetContextDocument(object doc)
  {
    if (doc is TransactionManager transactionManager)
    {
      this.transactionManager = transactionManager;
    }
    else if (doc is IRevitDocumentAggregateCache revitDocumentAggregateCache)
    {
      this.revitDocumentAggregateCache = revitDocumentAggregateCache;
    }
    else if (doc is IConvertedObjectsCache<Base, Element> receivedObjectsCache)
    {
      this.receivedObjectsCache = receivedObjectsCache;
    }
    else if (doc is IReceivedObjectIdMap<Base, Element> cache)
    {
      PreviouslyReceivedObjectIds = cache;
    }
    else if (doc is DB.View view)
    {
      // setting the view as a 2d view will result in no objects showing up, so only do it if it's a 3D view
      if (view is View3D view3D)
      {
        ViewSpecificOptions = new Options() { View = view, ComputeReferences = true };
      }
    }
    else if (doc is Document document)
    {
      Doc = document;
      DebugLog($"Using document: {Doc.PathName}");
      DebugLog($"Using units: {ModelUnits}");
    }
    else
    {
      throw new ArgumentException(
        $"Converter.{nameof(SetContextDocument)}() was passed an object of unexpected type, {doc.GetType()}"
      );
    }
  }

  const string DetailLevelCoarse = "Coarse";
  const string DetailLevelMedium = "Medium";
  const string DetailLevelFine = "Fine";
  public ViewDetailLevel DetailLevelSetting => GetDetailLevelSetting() ?? ViewDetailLevel.Fine;

  private ViewDetailLevel? GetDetailLevelSetting()
  {
    if (!Settings.TryGetValue("detail-level", out string detailLevel))
    {
      return null;
    }
    return detailLevel switch
    {
      DetailLevelCoarse => ViewDetailLevel.Coarse,
      DetailLevelMedium => ViewDetailLevel.Medium,
      DetailLevelFine => ViewDetailLevel.Fine,
      _ => null
    };
  }

  //NOTE: not all objects come from Revit, so their applicationId might be null, in this case we fall back on the Id
  //this fallback is only needed for a couple of ToNative conversions such as Floor, Ceiling, and Roof
  public void SetContextObjects(List<ApplicationObject> objects)
  {
    ContextObjects = new(objects.Count);
    foreach (var ao in objects)
    {
      var key = ao.applicationId ?? ao.OriginalId;
      if (ContextObjects.ContainsKey(key))
      {
        continue;
      }

      ContextObjects.Add(key, ao);
    }
  }

  public void SetPreviousContextObjects(List<ApplicationObject> objects)
  {
    //PreviousContextObjects = new(objects.Count);
    //foreach (var ao in objects)
    //{
    //  var key = ao.applicationId ?? ao.OriginalId;
    //  if (PreviousContextObjects.ContainsKey(key))
    //    continue;
    //  PreviousContextObjects.Add(key, ao);
    //}
  }




  // inside ConverterRevit
  private static Base CloneAsType(Base src, string fullTypeName /* e.g. "Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance, Speckle.Objects" */)
  {
    var t = Type.GetType(fullTypeName, throwOnError: false);
    if (t == null || !typeof(Base).IsAssignableFrom(t))
      return src; // type not present in the loaded kit -> keep the stock object

    var dst = (Base)Activator.CreateInstance(t)!;

    // Minimal identity copy
    dst.applicationId = src.applicationId;
    dst.id = src.id;

    // Copy matching public get/set props when names match
    var sProps = src.GetType().GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
    var dProps = t.GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                  .ToDictionary(p => p.Name);

    foreach (var sp in sProps)
    {
      if (!sp.CanRead) continue;
      if (dProps.TryGetValue(sp.Name, out var dp) && dp.CanWrite)
        dp.SetValue(dst, sp.GetValue(src));
    }

    // Copy dynamic members
    foreach (var kv in src.GetMembers(Speckle.Core.Models.DynamicBaseMemberType.All))
      dst[kv.Key] = kv.Value;

    return dst;
  }
  // REPLACE your WorkPlaneConnectionFamilyToSpeckle with this
  private Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance WorkPlaneConnectionFamilyToSpeckle(DB.FamilyInstance fi, out List<string> notes)
  {
    notes = new List<string>();

    var baseObj = FamilyInstanceToSpeckle(fi, out var innerNotes) as Objects.Other.Revit.RevitInstance;
    if (innerNotes?.Count > 0) notes.AddRange(innerNotes);
    if (baseObj == null)
      throw new Exception("FamilyInstanceToSpeckle did not return OR.RevitInstance");

    var r = new Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance
    {
      applicationId = baseObj.applicationId,
      id = baseObj.id
    };

    // copy dyanmic members except reserved
    foreach (var kv in baseObj.GetMembers(Speckle.Core.Models.DynamicBaseMemberType.All))
    {
      if (_reservedDynKeys.Contains(kv.Key)) continue;
      SetPropOrDyn(r, kv.Key, kv.Value);
    }

    // level uid
    if (fi.Document.GetElement(fi.LevelId) is DB.Level lvl)
      r.levelUniqueId = lvl.UniqueId;

    // sketch plane & work plane info
    var spId = fi.get_Parameter(DB.BuiltInParameter.SKETCH_PLANE_PARAM)?.AsElementId()
               ?? DB.ElementId.InvalidElementId;
    var sp = spId == DB.ElementId.InvalidElementId ? null : fi.Document.GetElement(spId) as DB.SketchPlane;

    r.isWorkPlaneHosted = sp != null;

    if (sp != null)
    {
      r.sketchPlaneUniqueId = sp.UniqueId;
      var pl = sp.GetPlane();

      var origin = new Objects.Geometry.Point(
        ScaleToSpeckle(pl.Origin.X),
        ScaleToSpeckle(pl.Origin.Y),
        ScaleToSpeckle(pl.Origin.Z)
      )
      { units = ModelUnits };

      r.workPlane = new Objects.Geometry.Plane(
        origin,
        new Objects.Geometry.Vector(pl.Normal.X, pl.Normal.Y, pl.Normal.Z),
        new Objects.Geometry.Vector(pl.XVec.X, pl.XVec.Y, pl.XVec.Z),
        new Objects.Geometry.Vector(pl.YVec.X, pl.YVec.Y, pl.YVec.Z)
      );

      var pref = sp.GetPlaneReference();
      if (pref != null)
      {
        r["workPlaneRefStable"] = pref.ConvertToStableRepresentation(fi.Document);
        r["workPlaneSourceElementUniqueId"] = fi.Document.GetElement(pref.ElementId)?.UniqueId;
      }
    }
    else
    {
      // fallback from transform
      var tr = fi.GetTransform();
      var origin = new Objects.Geometry.Point(
        ScaleToSpeckle(tr.Origin.X),
        ScaleToSpeckle(tr.Origin.Y),
        ScaleToSpeckle(tr.Origin.Z)
      )
      { units = ModelUnits };

      r.workPlane = new Objects.Geometry.Plane(
        origin,
        new Objects.Geometry.Vector(tr.BasisZ.X, tr.BasisZ.Y, tr.BasisZ.Z),
        new Objects.Geometry.Vector(tr.BasisX.X, tr.BasisX.Y, tr.BasisX.Z),
        new Objects.Geometry.Vector(tr.BasisY.X, tr.BasisY.Y, tr.BasisY.Z)
      );
    }

    // host offset
    double offFt = 0.0;
    var p1 = fi.get_Parameter(DB.BuiltInParameter.INSTANCE_ELEVATION_PARAM);
    var p2 = fi.get_Parameter(DB.BuiltInParameter.INSTANCE_FREE_HOST_OFFSET_PARAM);
    if (p1 != null) offFt = p1.AsDouble();
    if (p2 != null) offFt = p2.AsDouble();
    r.hostOffset = ScaleToSpeckle(offFt); // r.hostOffset inherits ModelUnits context

    // placement point + rotation
    if (fi.Location is DB.LocationPoint lp)
    {
      r.placementPoint = new Objects.Geometry.Point(
        ScaleToSpeckle(lp.Point.X),
        ScaleToSpeckle(lp.Point.Y),
        ScaleToSpeckle(lp.Point.Z)
      )
      { units = ModelUnits };

      r.rotation = lp.Rotation;
    }

    r.mirrored = fi.Mirrored;
    var f = fi.FacingOrientation; r.facingOrientation = new Objects.Geometry.Vector(f.X, f.Y, f.Z);
    var h = fi.HandOrientation; r.handOrientation = new Objects.Geometry.Vector(h.X, h.Y, h.Z);

    // nice names + required symbol info
    var symbol = fi.Symbol;
    var famName = symbol?.FamilyName ?? fi.Symbol?.Family?.Name ?? "Family";
    var typeName = symbol?.Name ?? fi.Name ?? "Type";
    var niceName = SanitizeRevitName($"{famName} - {typeName}");

    r.family = symbol?.FamilyName ?? symbol?.Family?.Name;
    r.type = symbol?.Name;
    r.category = fi.Category?.Name;
    r.builtInCategory = fi.Category?.Id.IntegerValue;

    r["name"] = niceName;
    r["displayName"] = niceName;
    r["familyName"] = famName;
    r["typeName"] = typeName;

    if (baseObj["symbol"] is Objects.BuiltElements.Revit.RevitSymbolElementType sym)
      r.symbol = sym;
    else
      r.symbol = ElementTypeToSpeckle(symbol) as Objects.BuiltElements.Revit.RevitSymbolElementType;

    // hydrate displayValue names if present
    if (r.displayValue != null)
      foreach (var m in r.displayValue) m["name"] = niceName;
    else if (r["displayValue"] is IEnumerable<Objects.Geometry.Mesh> dynMeshes)
      foreach (var m in dynMeshes) m["name"] = niceName;

    return r;
  }

  private static string SanitizeRevitName(string s)
  {
    if (string.IsNullOrEmpty(s)) return s;
    // conservative blocklist used by Revit name validators
    var bad = new HashSet<char>(":{}[]|;<>?\\\"'=`~".ToCharArray());
    var sb = new System.Text.StringBuilder(s.Length);
    foreach (var ch in s)
      sb.Append(bad.Contains(ch) ? '_' : ch);
    return sb.ToString().Trim();
  }






  // helpers

  // Find a structural column whose (string) parameter "applicationId" matches the given id.
  private Element FindHostColumnByApplicationId(string appId)
  {
    if (string.IsNullOrWhiteSpace(appId)) return null;

    var columns = new FilteredElementCollector(Doc)
      .OfCategory(BuiltInCategory.OST_StructuralColumns)
      .WhereElementIsNotElementType()
      .ToElements();

    foreach (var e in columns)
    {
      var v = GetParamString(e, "applicationId")
           ?? GetParamString(e, "ApplicationId")
           ?? GetParamString(e, "SpeckleApplicationId");
      if (string.Equals(v, appId, StringComparison.OrdinalIgnoreCase))
        return e;
    }
    return null;
  }

  private static string GetParamString(Element e, string name)
  {
    var p = e.LookupParameter(name);
    if (p == null) return null;
    try
    {
      if (p.StorageType == StorageType.String) return p.AsString();
      return p.AsValueString(); // fallback for non-string storage
    }
    catch { return null; }
  }

  private static XYZ ElementCenter(Element e)
  {
    var bb = e.get_BoundingBox(null);
    if (bb == null) return XYZ.Zero;
    return (bb.Min + bb.Max) * 0.5;
  }

  // Reuse your existing face-picking logic, but with an explicit host Element
  private Reference GetNearestPlanarFaceReference(Element host, XYZ nearPoint)
  {
    var op = new Options { ComputeReferences = true };
    var ge = host.get_Geometry(op);
    Reference faceRef = null;
    double planeDist = double.MaxValue;
    GetReferencePlane(ge, nearPoint, ref faceRef, ref planeDist);
    return faceRef;
  }




















  // reserved dynamic keys that must never be written
  private static readonly HashSet<string> _reservedDynKeys = new(StringComparer.OrdinalIgnoreCase)
{
  "speckle_type","applicationId","id","units","bbox","__closure",
  // also skip keys you have typed props for:
  "family","type","symbol","category","builtInCategory"
};

  // set strongly-typed property if it exists; convert types if needed.
  // only use dynamic when NO property exists.
  private static void SetPropOrDyn(Base dst, string name, object value)
  {
    var p = dst.GetType().GetProperty(
      name,
      System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);

    if (p != null)
    {
      try
      {
        if (value == null) { p.SetValue(dst, null); return; }

        var target = Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;
        var v = value;

        if (!target.IsInstanceOfType(v))
        {
          // handle numerics / strings -> numerics / enums / bool, etc.
          if (target.IsEnum && v is string sEnum)
            v = Enum.Parse(target, sEnum, ignoreCase: true);
          else
            v = Convert.ChangeType(v, target);
        }

        p.SetValue(dst, v);
      }
      catch
      {
        // swallow: if conversion fails, we simply don't set anything
        // (but we also DO NOT write a dynamic with the same name)
      }
      return;
    }

    if (_reservedDynKeys.Contains(name)) return; // don’t shadow known keys
    dst[name] = value;
  }















  public void SetConverterSettings(object settings)
  {
    Settings = settings as Dictionary<string, string>;
  }

  public Base ConvertToSpeckle(object @object)
  {
    Base returnObject = null;
    List<string> notes = new();
    string id = @object is Element element ? element.UniqueId : string.Empty;
    try
    {
      if (@object is DB.Element e)
      {
        Report.Log($"[CTS] pick id:{e.UniqueId} cat:{e.Category?.Name} bic:{e.Category?.Id.IntegerValue}");
      }
    }
    catch { }

    switch (@object)
    {
      case DB.Document o:
        returnObject = ModelToSpeckle(o, !o.IsFamilyDocument);
        break;
      case DB.DetailCurve o:
        returnObject = DetailCurveToSpeckle(o);
        break;
      case DB.DirectShape o:
        returnObject = DirectShapeToSpeckle(o);
        break;
      case DB.FamilyInstance o
  when o.Category?.Id.IntegerValue == (int)DB.BuiltInCategory.OST_StructConnections:
        DebugLog($"[CTS] hit struct-conn: {o.UniqueId}");
        returnObject = WorkPlaneConnectionFamilyToSpeckle(o, out notes);
        DebugLog($"[CTS] OUT type: {returnObject?.speckle_type}");
        break;

      case DB.FamilyInstance o:
        returnObject = FamilyInstanceToSpeckle(o, out notes);
        break;
      case DB.Floor o:
        returnObject = FloorToSpeckle(o, out notes);
        break;
      case DB.FabricationPart o:
        returnObject = FabricationPartToSpeckle(o, out notes);
        break;
      case DB.Group o:
        returnObject = GroupToSpeckle(o);
        break;
      case DB.Level o:
        returnObject = LevelToSpeckle(o);
        break;
      case DB.View o:
        returnObject = ViewToSpeckle(o);
        break;
      //NOTE: Converts all materials in the materials library
      case DB.Material o:
        returnObject = ConvertAndCacheMaterial(o.Id, o.Document);
        break;
      case DB.ModelCurve o:
        if ((BuiltInCategory)o.Category.Id.IntegerValue == BuiltInCategory.OST_RoomSeparationLines)
        {
          returnObject = RoomBoundaryLineToSpeckle(o);
        }
        else if ((BuiltInCategory)o.Category.Id.IntegerValue == BuiltInCategory.OST_MEPSpaceSeparationLines)
        {
          returnObject = SpaceSeparationLineToSpeckle(o);
        }
        else
        {
          returnObject = ModelCurveToSpeckle(o);
        }

        break;
      case DB.Opening o:
        returnObject = OpeningToSpeckle(o);
        break;
      case DB.RoofBase o:
        returnObject = RoofToSpeckle(o, out notes);
        break;
      case DB.Area o:
        returnObject = AreaToSpeckle(o);
        break;
      case DB.Architecture.Room o:
        returnObject = RoomToSpeckle(o);
        break;
      case DB.Architecture.TopographySurface o:
        returnObject = TopographyToSpeckle(o);
        break;
      case DB.Wall o:
        returnObject = WallToSpeckle(o, out notes);
        break;
      case DB.Mechanical.Duct o:
        returnObject = DuctToSpeckle(o, out notes);
        break;
      case DB.Mechanical.FlexDuct o:
        returnObject = DuctToSpeckle(o);
        break;
      case DB.Mechanical.Space o:
        returnObject = SpaceToSpeckle(o);
        break;
      case DB.Plumbing.Pipe o:
        returnObject = PipeToSpeckle(o);
        break;
      case DB.Plumbing.FlexPipe o:
        returnObject = PipeToSpeckle(o);
        break;
      case DB.Electrical.Wire o:
        returnObject = WireToSpeckle(o);
        break;
      case DB.Electrical.CableTray o:
        returnObject = CableTrayToSpeckle(o);
        break;
      case DB.Electrical.Conduit o:
        returnObject = ConduitToSpeckle(o);
        break;
      //these should be handled by curtain walls
      case DB.CurtainGridLine _:
        throw new ConversionSkippedException(
          "Curtain Grid Lines are handled as part of the parent CurtainWall conversion"
        );
      case DB.Architecture.BuildingPad o:
        returnObject = BuildingPadToSpeckle(o);
        break;
      case DB.Architecture.Stairs o:
        returnObject = StairToSpeckle(o);
        break;
      //these are handled by Stairs
      case DB.Architecture.StairsRun _:
        throw new ConversionSkippedException($"{nameof(StairsRun)} are handled by the {nameof(Stairs)} conversion");
      case DB.Architecture.StairsLanding _:
        throw new ConversionSkippedException($"{nameof(StairsLanding)} are handled by the {nameof(Stairs)} conversion");
        break;
      case DB.Architecture.Railing o:
        returnObject = RailingToSpeckle(o);
        break;
      case DB.Architecture.TopRail o:
        returnObject = TopRailToSpeckle(o);
        break;
      case DB.Architecture.HandRail _:
        throw new ConversionSkippedException($"{nameof(HandRail)} are handled by the {nameof(Railing)} conversion");
      case DB.Structure.Rebar o:
        returnObject = RebarToSpeckle(o);
        break;
      case DB.Ceiling o:
        returnObject = CeilingToSpeckle(o, out notes);
        break;
      case DB.PointCloudInstance o:
        returnObject = PointcloudToSpeckle(o);
        break;
      case DB.ProjectInfo o:
        returnObject = ProjectInfoToSpeckle(o);
        break;
      case DB.ElementType o:
        returnObject = ElementTypeToSpeckle(o);
        break;
      case DB.Grid o:
        returnObject = GridLineToSpeckle(o);
        break;
      case DB.ReferencePoint o:
        if ((BuiltInCategory)o.Category.Id.IntegerValue == BuiltInCategory.OST_AnalyticalNodes)
        {
          returnObject = AnalyticalNodeToSpeckle(o);
        }

        break;
      case DB.Structure.BoundaryConditions o:
        returnObject = BoundaryConditionsToSpeckle(o);
        break;
      case DB.Structure.StructuralConnectionHandler o:
        returnObject = StructuralConnectionHandlerToSpeckle(o);
        break;
      case DB.CombinableElement o:
        returnObject = CombinableElementToSpeckle(o);
        break;

      // toposolid from Revit 2024
#if (REVIT2024)
      case DB.Toposolid o:
        returnObject = ToposolidToSpeckle(o, out notes);
        break;
#endif
#if REVIT2020 || REVIT2021 || REVIT2022
      case DB.Structure.AnalyticalModelStick o:
        returnObject = AnalyticalStickToSpeckle(o);
        break;
      case DB.Structure.AnalyticalModelSurface o:
        returnObject = AnalyticalSurfaceToSpeckle(o);
        break;
#else
      case DB.Structure.AnalyticalMember o:
        returnObject = AnalyticalStickToSpeckle(o);
        break;
      case DB.Structure.AnalyticalPanel o:
        returnObject = AnalyticalSurfaceToSpeckle(o);
        break;
#endif
      default:
        // if we don't have a direct conversion, still try to send this element as a generic RevitElement
        var el = @object as Element;
        if (el.IsElementSupported())
        {
          returnObject = RevitElementToSpeckle(el, out notes);
          break;
        }
        throw new NotSupportedException($"Conversion of {@object.GetType().Name} is not supported.");
    }

    // NOTE: Only try generic method assignment if there is no existing render material from conversions;
    // we might want to try later on to capture it more intelligently from inside conversion routines.
    if (
      returnObject != null
      && returnObject["renderMaterial"] == null
      && returnObject["displayValue"] == null
      && !(returnObject is Collection)
    )
    {
      if (GetElementRenderMaterial(@object as DB.Element) is RenderMaterial material)
      {
        returnObject["renderMaterial"] = material;
      }
    }

    // log
    if (Report.ReportObjects.TryGetValue(id, out var reportObj) && notes.Count > 0)
    {
      reportObj.Update(log: notes);
    }

    return returnObject;
  }

  private string GetElemInfo(object o)
  {
    if (o is Element e)
    {
      return $", name: {e.Name}, id: {e.UniqueId}";
    }

    return "";
  }

  private Base SwapGeometrySchemaObject(Base @object)
  {
    // schema check
    var speckleSchema = @object["@SpeckleSchema"] as Base;
    if (speckleSchema == null || !CanConvertToNative(speckleSchema))
    {
      return @object; // Skip if no schema, or schema is non-convertible.
    }

    // Invert the "Geometry->SpeckleSchema" to be the logical "SpeckleSchema -> Geometry" order.
    // This is caused by RhinoBIM, the MappingTool in rhino, and any Grasshopper Schema node with the option active.
    if (speckleSchema is BER.DirectShape ds)
    {
      // HACK: This is an explicit exception for DirectShapes. This is the only object class that does not have a
      // `SchemaMainParam`, which means the swap performed below would not work.
      // In this case, we cast directly and "unwrap" the schema object manually, setting the Brep as the only
      // item in the list.
      ds.baseGeometries = new List<Base> { @object };
    }
    else if (speckleSchema is MappedBlockWrapper mbw)
    {
      if (@object is not BlockInstance bi)
      {
        throw new Exception($"{nameof(MappedBlockWrapper)} can only be used with {nameof(BlockInstance)} objects.");
      }

      mbw.instance = bi;
    }
    else
    {
      // find self referential prop and set value to @object if it is null (happens when sent from gh)
      // if you can find a "MainParamProperty" get that
      // HACK: The results of this can be inconsistent as we don't really know which is the `MainParamProperty`, that is info that is attached to the constructor input. Hence the hack above ☝🏼
      var prop = speckleSchema
        .GetInstanceMembers()
        .Where(o => speckleSchema[o.Name] == null)
        .FirstOrDefault(o => o.PropertyType.IsInstanceOfType(@object));
      if (prop != null)
      {
        speckleSchema[prop.Name] = @object;
      }
    }
    return speckleSchema;
  }

  public object ConvertToNative(Base @base)
  {
    var nativeObject = ConvertToNativeObject(@base);

    switch (nativeObject)
    {
      case ApplicationObject appObject:
        if (appObject.Converted.Cast<Element>().ToList() is List<Element> typedList && typedList.Count >= 1)
        {
          receivedObjectsCache?.AddConvertedObjects(@base, typedList);
        }
        break;
      case Element element:
        receivedObjectsCache?.AddConvertedObjects(@base, new List<Element> { element });
        break;
    }

    return nativeObject;
  }

  public object ConvertToNativeObject(Base @object)
  {
    // Get setting for if the user is only trying to preview the geometry
    Settings.TryGetValue("preview", out string isPreview);
    if (bool.Parse(isPreview ?? "false") == true)
    {
      return PreviewGeometry(@object);
    }

    // Get settings for receive direct meshes , assumes objects aren't nested like in Tekla Structures
    Settings.TryGetValue("recieve-objects-mesh", out string recieveModelMesh);
    if (bool.Parse(recieveModelMesh ?? "false"))
    {
      if ((@object is Other.Instance || @object.IsDisplayableObject()) && @object is not BE.Room)
      {
        return DisplayableObjectToNative(@object);
      }
      else
      {
        return null;
      }
    }

    //Family Document
    if (Doc.IsFamilyDocument)
    {
      switch (@object)
      {
        case ICurve o:
          return ModelCurveToNative(o);
        case Geometry.Brep o:
          return TryDirectShapeToNative(o, ToNativeMeshSettingEnum.Default);
        case Geometry.Mesh o:
          return TryDirectShapeToNative(o, ToNativeMeshSettingEnum.Default);
        case BER.FreeformElement o:
          return FreeformElementToNative(o);
        default:
          return null;
      }
    }

    // Check if object has inner `SpeckleSchema` prop and swap if appropriate
    @object = SwapGeometrySchemaObject(@object);

    switch (@object)
    {
      //geometry
      case ICurve o:
        return ModelCurveToNative(o);
      case Geometry.Brep o:
        return TryDirectShapeToNative(o, ToNativeMeshSetting);
      case Geometry.Mesh mesh:
        switch (ToNativeMeshSetting)
        {
          case ToNativeMeshSettingEnum.DxfImport:
            return MeshToDxfImport(mesh, Doc);
          case ToNativeMeshSettingEnum.DxfImportInFamily:
            return MeshToDxfImportFamily(mesh, Doc);
          case ToNativeMeshSettingEnum.Default:
          default:
            return TryDirectShapeToNative(mesh, ToNativeMeshSettingEnum.Default);
        }
      // non revit built elems
      case BE.Alignment o:
        if (o.curves is null && o.baseCurve is not null) // This is for backwards compatibility for the deprecated basecurve property
        {
          return ModelCurveToNative(o.baseCurve);
        }

        return AlignmentToNative(o);

      case BE.Structure o:
        return TryDirectShapeToNative(
          new ApplicationObject(o.id, o.speckle_type) { applicationId = o.applicationId },
          o.displayValue,
          ToNativeMeshSetting
        );
      //built elems
      case BER.AdaptiveComponent o:
        return AdaptiveComponentToNative(o);

      //case BE.TeklaStructures.TeklaBeam o:
      //  return TeklaBeamToNative(o);

      case BE.Beam o:
        return BeamToNative(o);

      case BE.Brace o:
        return BraceToNative(o);

      case BE.Column o:
        return ColumnToNative(o);

#if REVIT2020  || REVIT2021
#else
      case BE.Ceiling o:
        return CeilingToNative(o);
#endif

      case BERC.DetailCurve o:
        return DetailCurveToNative(o);

      case BER.DirectShape o:
        try
        {
          // Try to convert to direct shape, taking into account the current mesh settings
          return DirectShapeToNative(o, ToNativeMeshSetting);
        }
        catch (FallbackToDxfException e)
        {
          // FallbackToDxf exception means we should attempt a DXF import instead.
          switch (ToNativeMeshSetting)
          {
            case ToNativeMeshSettingEnum.DxfImport:
              return DirectShapeToDxfImport(o); // DirectShape -> DXF
            case ToNativeMeshSettingEnum.DxfImportInFamily:
              return DirectShapeToDxfImportFamily(o); // DirectShape -> Family (DXF inside)
            case ToNativeMeshSettingEnum.Default:
            default:
              // For anything else, try again with the default fallback (ugly meshes).
              return DirectShapeToNative(o, ToNativeMeshSettingEnum.Default);
          }
        }

      case BER.FreeformElement o:
        return FreeformElementToNative(o);

      case BER.FamilyInstance o:
        return FamilyInstanceToNative(o);

      case BE.Network o:
        return NetworkToNative(o);

      case BE.Floor o:
        return FloorToNative(o);
      case BE.Level o:
        return LevelToNative(o);

      case BERC.ModelCurve o:
        return ModelCurveToNative(o);

      case BE.Opening o:
        return OpeningToNative(o);

      case BERC.RoomBoundaryLine o:
        return RoomBoundaryLineToNative(o);

      case BERC.SpaceSeparationLine o:
        return SpaceSeparationLineToNative(o);

      case BER.RevitRebarGroup o:
        return RebarToNative(o);

      case BE.Roof o:
        return RoofToNative(o);

      case BE.Topography o:
        return TopographyToNative(o);

      case BER.RevitCurtainWallPanel o:
        return PanelToNative(o);

      case BER.RevitProfileWall o:
        return ProfileWallToNative(o);

      case BER.RevitFaceWall o:
#if REVIT2021 || REVIT2022
        return FaceWallToNative(o);
#else
        return FaceWallToNativeV2(o);
#endif

      case BE.Wall o:
        return WallToNative(o);

      case BE.Duct o:
        return DuctToNative(o);

      case BE.Pipe o:
        return PipeToNative(o);

      case BE.Wire o:
        return WireToNative(o);

      case BE.CableTray o:
        return CableTrayToNative(o);

      case BE.Conduit o:
        return ConduitToNative(o);

      case BE.Revit.RevitRailing o:
        return RailingToNative(o);

      case BER.ParameterUpdater o:
        return UpdateParameter(o);

      case BE.View3D o:
        return ViewToNative(o);

      case RevitMEPFamilyInstance o:
        return FittingOrMEPInstanceToNative(o);

      case Other.Revit.RevitInstance o:
        return RevitInstanceToNative(o);

      case BE.Room o:
        return RoomToNative(o);

      case BE.GridLine o:
        return GridLineToNative(o);

      case DataTable o:
        return DataTableToNative(o);

      case BE.Space o:
        return SpaceToNative(o);
      //Structural
      case STR.Geometry.Element1D o:
        return AnalyticalStickToNative(o);

      case STR.Geometry.Element2D o:
        return AnalyticalSurfaceToNative(o);

      case STR.Geometry.Node o:
        return AnalyticalNodeToNative(o);

      case STR.Analysis.Model o:
        return StructuralModelToNative(o);

      // other
      case Other.BlockInstance o:
        return BlockInstanceToNative(o);

      case Other.MappedBlockWrapper o:
        return MappedBlockWrapperToNative(o);
      // gis
      case PolygonElement o:
        return PolygonElementToNative(o);

      case GisFeature o:
        return GisFeatureToNative(o);

#if (REVIT2024)
      case RevitToposolid o:
        return ToposolidToNative(o);
#endif

      //hacky but the current comments camera is not a Base object
      //used only from DUI and not for normal geometry conversion
      case Base b:
        //hacky but the current comments camera is not a Base object
        //used only from DUI and not for normal geometry conversion
        var boo = b["isHackySpeckleCamera"] as bool?;
        if (boo == true)
        {
          return ViewOrientation3DToNative(b);
        }

        return null;

      default:
        return null;
    }
  }
  // inside RevitInstanceToNative(...) after you've got `familyInstance` and called Doc.Regenerate()

  public object ConvertToNativeDisplayable(Base @base)
  {
    var nativeObject = DisplayableObjectToNative(@base);
    if (nativeObject.Converted.Cast<Element>().ToList() is List<Element> typedList && typedList.Count >= 1)
    {
      receivedObjectsCache?.AddConvertedObjects(@base, typedList);
    }
    return nativeObject;
  }







  public ApplicationObject RevitInstanceToNative(Objects.Other.Revit.RevitInstance instance, ApplicationObject appObj = null)
  {
    // --- local helpers to read dynamic/typed flags safely ---
    static bool TryGetDynBool(Base b, string key, out bool value)
    {
      value = false;
      if (b == null) return false;
      if (!b.GetMembers(Speckle.Core.Models.DynamicBaseMemberType.All).TryGetValue(key, out var raw) || raw == null)
        return false;

      switch (raw)
      {
        case bool bb: value = bb; return true;
        case string s when bool.TryParse(s, out var bv): value = bv; return true;
        case string s2 when int.TryParse(s2, out var iv): value = iv != 0; return true;
        case int i: value = i != 0; return true;
        case long l: value = l != 0; return true;
        case double d: value = Math.Abs(d) > 1e-9; return true;
        default: return false;
      }
    }
    static bool WantsVerticalFlip(Base b)
    {
      bool typed = false, hasTyped = false;
      if (b is Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance rwpi)
      {
        typed = rwpi.flipVertical; // if the local kit has the property
        hasTyped = true;
      }
      if (TryGetDynBool(b, "flipVertical", out var dyn))
        return dyn;
      return hasTyped && typed;
    }
    // --------------------------------------------------------

    DB.FamilyInstance familyInstance = null;
    var docObj = GetExistingElementByApplicationId(instance?.applicationId);
    appObj ??= new ApplicationObject(instance?.id, instance?.speckle_type) { applicationId = instance?.applicationId };
    var isUpdate = false;

    if (instance == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed, logItem: "Instance was null.");
      return appObj;
    }

    if (IsIgnore(docObj, appObj))
      return appObj;

    // ---- resolve symbol ----
    var def =
        instance.typedDefinition as Objects.BuiltElements.Revit.RevitSymbolElementType
        ?? instance.definition as Objects.BuiltElements.Revit.RevitSymbolElementType
        ?? instance["symbol"] as Objects.BuiltElements.Revit.RevitSymbolElementType;

    DB.FamilySymbol familySymbol = null;
    bool isExactMatch = false;

    if (def != null)
      familySymbol = GetElementType<DB.FamilySymbol>(def, appObj, out isExactMatch);

    if (familySymbol == null)
    {
      var fam = instance["family"] as string;
      var typ = instance["type"] as string;
      if (!string.IsNullOrWhiteSpace(fam) && !string.IsNullOrWhiteSpace(typ))
        familySymbol = FindFamilySymbolByName(fam, typ);
    }

    if (familySymbol == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed,
                    logItem: $"Family/type not found. family='{instance["family"] ?? "<null>"}' type='{instance["type"] ?? "<null>"}'.");
      return appObj;
    }

    if (!familySymbol.IsActive)
      familySymbol.Activate();

    // ---- level ----
    DB.Level level = ConvertLevelToRevit(instance.level, out ApplicationObject.State _);
    if (level == null)
    {
      level = FallbackLevel(instance["levelUniqueId"] as string) ?? CreateLevelIfNone();
      if (level == null)
      {
        appObj.Update(status: ApplicationObject.State.Failed, logItem: "No Level found or could be created.");
        return appObj;
      }
    }

    // ---- placement type ----
    string placementStr = (def?.placementType ?? instance["placementType"] as string) ?? string.Empty;
    var placement = Enum.TryParse<DB.FamilyPlacementType>(placementStr, true, out var placementType)
                      ? placementType
                      : familySymbol.Family?.FamilyPlacementType ?? DB.FamilyPlacementType.Invalid;

    bool hasPlacementPoint = instance["placementPoint"] is Objects.Geometry.Point;
    bool hasTransform = instance.transform != null;
    bool isWorkPlaneBased = placement == DB.FamilyPlacementType.WorkPlaneBased;
    bool forceNamedWorkPlane = isWorkPlaneBased && (hasPlacementPoint || hasTransform);

    // ---- yaw (radians) from payload ----
    double? yawRad = null;
    if (instance is Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance rwpi0)
      yawRad = rwpi0.rotation;
    if (!yawRad.HasValue && instance["rotation"] is double rotVal)
      yawRad = rotVal;

    DebugLog($"[RVTIN] instance:{instance.applicationId} fam:'{familySymbol.FamilyName}' type:'{familySymbol.Name}' " +
             $"placement:{placement} (raw:'{placementStr}') level:{level?.Name ?? "<null>"} " +
             $"hasPlacementPoint:{hasPlacementPoint} hasTransform:{hasTransform} WPBased:{isWorkPlaneBased} yaw(rad):{(yawRad?.ToString() ?? "<none>")}");

    // ---------------- Insertion point (external -> internal) ----------------
    XYZ insertionExt;
    bool usedPlacementPoint = false;

    if (hasPlacementPoint)
    {
      var pp = (Objects.Geometry.Point)instance["placementPoint"];
      var u = UnitsOrModel(pp.units);
      insertionExt = new XYZ(
        ScaleToNative(pp.x, u),
        ScaleToNative(pp.y, u),
        ScaleToNative(pp.z, u)
      );
      usedPlacementPoint = true;
      DebugLog($"[RVTIN] placementPoint external (scaled): {insertionExt}  units:{u}");
    }
    else if (hasTransform)
    {
      var t = TransformToNative(instance.transform);
      insertionExt = t.OfPoint(XYZ.Zero);
      usedPlacementPoint = false;
      DebugLog($"[RVTIN] insertion from transform (already internal): {insertionExt}");
    }
    else
    {
      insertionExt = XYZ.Zero;
      usedPlacementPoint = true;
      DebugLog($"[RVTIN] no point/transform -> using ZERO external");
    }

    var docT = GetDocReferencePointTransform(Doc);
    var insertionPoint = usedPlacementPoint ? docT.OfPoint(insertionExt) : insertionExt;

    // ---------------- UPDATE PATH ----------------
    if (docObj != null)
    {
      try
      {
        var revitType = Doc.GetElement(docObj.GetTypeId()) as ElementType;

        if (revitType == null || familySymbol.FamilyName != revitType.FamilyName)
        {
          DebugLog($"[RVTIN] Existing type family mismatch -> deleting {docObj.Id.IntegerValue}");
          Doc.Delete(docObj.Id);
        }
        else
        {
          familyInstance = (DB.FamilyInstance)docObj;
          DebugLog($"[RVTIN] Updating existing uid:{familyInstance.UniqueId} host:{familyInstance.Host?.Id.IntegerValue}");

          if (forceNamedWorkPlane)
          {
            DebugLog("[RVTIN] WorkPlaneBased + plane may change -> recreate to reset host plane.");
            Doc.Delete(docObj.Id);
            familyInstance = null;
          }
          else
          {
            var newPt = new XYZ(insertionPoint.X, insertionPoint.Y, (familyInstance.Location as DB.LocationPoint).Point.Z);
            (familyInstance.Location as DB.LocationPoint).Point = newPt;
            if ((familyInstance.Location as DB.LocationPoint).Point != newPt)
              (familyInstance.Location as DB.LocationPoint).Point = newPt;

            if (isExactMatch && revitType.Id.IntegerValue != familySymbol.Id.IntegerValue)
            {
              DebugLog($"[RVTIN] Type change (old:{revitType.Id.IntegerValue}, new:{familySymbol.Id.IntegerValue})");
              familyInstance.ChangeTypeId(familySymbol.Id);
            }

            TrySetParam(familyInstance, DB.BuiltInParameter.FAMILY_LEVEL_PARAM, level);
            TrySetParam(familyInstance, DB.BuiltInParameter.FAMILY_BASE_LEVEL_PARAM, level);
          }
        }
        isUpdate = familyInstance != null;
      }
      catch (Autodesk.Revit.Exceptions.ApplicationException ex)
      {
        DebugLog($"[RVTIN] Update path threw, will recreate. {ex.GetType().Name}: {ex.Message}");
      }
    }

    // ---------------- CREATE PATH ----------------
    if (familyInstance == null)
    {
      if (forceNamedWorkPlane)
      {
        DB.ReferencePlane rp = null;
        DB.SketchPlane sp = null;

        DB.View viewForRp = Doc.ActiveView;
        if (viewForRp == null || viewForRp.IsTemplate)
        {
          viewForRp = new FilteredElementCollector(Doc)
                      .OfClass(typeof(DB.ViewPlan))
                      .Cast<DB.ViewPlan>()
                      .FirstOrDefault(v => !v.IsTemplate);
        }

        try
        {
          var rotT = DB.Transform.CreateRotation(DB.XYZ.BasisZ, yawRad ?? 0.0);
          var yDir = rotT.OfVector(DB.XYZ.BasisY);
          var bubble = insertionPoint;
          var free = insertionPoint + yDir;

          // we no longer try to flip by changing plane normal; rotation will handle flip
          var cut = DB.XYZ.BasisZ;

          rp = Doc.Create.NewReferencePlane(bubble, free, cut, viewForRp ?? Doc.ActiveView);
          rp.Name = $"SPK_{Guid.NewGuid():N}".Substring(0, 8);
        }
        catch (Exception ex)
        {
          DebugLog($"[RVTIN] ReferencePlane creation failed: {ex.GetType().Name}: {ex.Message}");
        }

        try
        {
#if REVIT2019 || REVIT2020 || REVIT2021 || REVIT2022 || REVIT2023 || REVIT2024 || REVIT2025
          sp = (rp != null)
               ? DB.SketchPlane.Create(Doc, rp.Id)
               : DB.SketchPlane.Create(Doc, DB.Plane.CreateByNormalAndOrigin(DB.XYZ.BasisX, insertionPoint));
#else
        sp = (rp != null)
             ? DB.SketchPlane.Create(Doc, rp)
             : DB.SketchPlane.Create(Doc, DB.Plane.CreateByNormalAndOrigin(DB.XYZ.BasisX, insertionPoint));
#endif
        }
        catch (Exception ex)
        {
          DebugLog($"[RVTIN] SketchPlane creation failed: {ex.GetType().Name}: {ex.Message}");
        }

        if (sp != null)
        {
          try
          {
            familyInstance = Doc.Create.NewFamilyInstance(
              insertionPoint, familySymbol, sp, DB.Structure.StructuralType.NonStructural);

            var skParam = familyInstance.get_Parameter(DB.BuiltInParameter.SKETCH_PLANE_PARAM);
            if (skParam != null && !skParam.IsReadOnly) skParam.Set(sp.Id);
          }
          catch (Exception ex)
          {
            DebugLog($"[RVTIN] NewFamilyInstance(SketchPlane) failed: {ex.GetType().Name}: {ex.Message}");
          }
        }

        if (familyInstance == null)
        {
          try
          {
            familyInstance = Doc.Create.NewFamilyInstance(
              insertionPoint, familySymbol, level, DB.Structure.StructuralType.NonStructural);
          }
          catch (Exception ex)
          {
            DebugLog($"[RVTIN] Fallback placement failed: {ex.GetType().Name}: {ex.Message}");
          }
        }
      }
      else
      {
        switch (placement)
        {
          case DB.FamilyPlacementType.OneLevelBasedHosted when CurrentHostElement != null:
            familyInstance = Doc.Create.NewFamilyInstance(
              insertionPoint, familySymbol, CurrentHostElement, level, DB.Structure.StructuralType.NonStructural);
            break;

          case DB.FamilyPlacementType.WorkPlaneBased when CurrentHostElement != null:
            {
              var op = new DB.Options { ComputeReferences = true };
              var geomElement = CurrentHostElement.get_Geometry(op);
              if (geomElement == null)
              {
                Doc.Regenerate();
                geomElement = CurrentHostElement.get_Geometry(op);
              }
              if (geomElement == null) goto default;

              DB.Reference faceRef = null;
              var planeDist = double.MaxValue;
              GetReferencePlane(geomElement, insertionPoint, ref faceRef, ref planeDist);

              familyInstance = Doc.Create.NewFamilyInstance(faceRef, insertionPoint, new DB.XYZ(0, 0, 0), familySymbol);

              var lvlParams = familyInstance.GetParameters("Schedule Level");
              if (lvlParams?.Count > 0 && level != null) lvlParams[0].Set(level.Id);
              break;
            }

          default:
            familyInstance = Doc.Create.NewFamilyInstance(
              insertionPoint, familySymbol, level, DB.Structure.StructuralType.NonStructural);
            break;
        }
      }
    }

    if (familyInstance == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed, logItem: "Could not create instance.");
      return appObj;
    }

    Doc.Regenerate();


/*//    Tilt the clip vertically by 90° (axis lies in the work plane):

TryRotate90(Doc, familyInstance, AxisKind.InPlaneX, DebugLog); // or InPlaneY


    // Quarter - turn in plan(spin around Z):

    TryRotate90(Doc, familyInstance, AxisKind.WorldZ, DebugLog);


    //Roll 90° around the instance’s own “up”:

    TryRotate90(Doc, familyInstance, AxisKind.InstanceZ, DebugLog);
*/


    var shouldFlip = WantsVerticalFlip(instance);

    if (shouldFlip && familyInstance?.Location is DB.LocationPoint lp)
    {
      using (var st = new DB.SubTransaction(Doc))
      {
        st.Start();

        bool rePin = false;
        try { if (familyInstance.Pinned) { familyInstance.Pinned = false; rePin = true; } }
        catch (Exception ex) { DebugLog($"[VFlip] Unpin failed: {ex.Message}"); }

        // Horizontal plane through insertion point (normal = WORLD Z) -> flips up/down
        var plane = DB.Plane.CreateByNormalAndOrigin(DB.XYZ.BasisZ, lp.Point);

        try
        {
          DB.ElementTransformUtils.MirrorElements(
            Doc,
            new List<DB.ElementId> { familyInstance.Id },
            plane,
            false // mirrorCopies
          );
          DebugLog("[VFlip] Mirrored across world-Z (horizontal) plane.");
        }
        catch (Exception ex)
        {
          DebugLog($"[VFlip] MirrorElements failed: {ex.GetType().Name}: {ex.Message}");

          // Fallback: 180° rotate around a WORLD-HORIZONTAL axis through the insertion point
          try
          {
            var axis = DB.Line.CreateUnbound(lp.Point, DB.XYZ.BasisX); // BasisY also works
            DB.ElementTransformUtils.RotateElement(Doc, familyInstance.Id, axis, Math.PI);
            DebugLog("[VFlip] Fallback 180° rotate around world X succeeded.");
          }
          catch (Exception ex2)
          {
            DebugLog($"[VFlip] Fallback rotate failed: {ex2.GetType().Name}: {ex2.Message}");
          }
        }

        // Try to re-add void cut (common for structural connections) and log outcome
        try
        {
          var hostElem = familyInstance.Host as DB.Element;
          if (hostElem != null)
          {
            DB.InstanceVoidCutUtils.AddInstanceVoidCut(Doc, hostElem, familyInstance);
            DebugLog("[VFlip] AddInstanceVoidCut attempted.");
          }
          else
          {
            DebugLog("[VFlip] No host to cut.");
          }
        }
        catch (Exception ex) { DebugLog($"[VFlip] AddInstanceVoidCut failed: {ex.Message}"); }

        try { if (rePin) familyInstance.Pinned = true; }
        catch (Exception ex) { DebugLog($"[VFlip] Re-pin failed: {ex.Message}"); }

        st.Commit();
      }
    }




    if (!isWorkPlaneBased)
    {
      if (yawRad.HasValue && Math.Abs(yawRad.Value) > 1e-9 && familyInstance.Location is DB.LocationPoint lpt)
      {
        var axis = DB.Line.CreateUnbound(lpt.Point, DB.XYZ.BasisZ);
        try
        {
          DB.ElementTransformUtils.RotateElement(Doc, familyInstance.Id, axis, yawRad.Value);
        }
        catch (Autodesk.Revit.Exceptions.ApplicationException e)
        {
          appObj.Update(logItem: $"Could not rotate instance by 'rotation': {e.Message}");
        }
      }
    }

    // Hand / Facing / standard mirror
    if (familyInstance.CanFlipHand && instance.handFlipped != familyInstance.HandFlipped) familyInstance.flipHand();
    if (familyInstance.CanFlipFacing && instance.facingFlipped != familyInstance.FacingFlipped) familyInstance.flipFacing();
    if (instance.mirrored != familyInstance.Mirrored)
    {
      DB.Group group = null;
      try { group = CurrentHostElement != null ? Doc.Create.NewGroup(new[] { familyInstance.Id }) : null; }
      catch (Autodesk.Revit.Exceptions.InvalidOperationException) { }

      var toMirror = group != null ? new[] { group.Id } : new[] { familyInstance.Id };
      try
      {
        DB.ElementTransformUtils.MirrorElements(
          Doc, toMirror, DB.Plane.CreateByNormalAndOrigin(DB.XYZ.BasisY, insertionPoint), false);
      }
      catch (Autodesk.Revit.Exceptions.ApplicationException e)
      {
        appObj.Update(logItem: $"Instance could not be mirrored: {e.Message}");
      }
      group?.UngroupMembers();
    }

    // diagnostics
    var hostId = familyInstance.Host?.Id.IntegerValue;
    var skPlaneId = familyInstance.get_Parameter(DB.BuiltInParameter.SKETCH_PLANE_PARAM)?.AsElementId();
    var skPlaneStr = (skPlaneId != null && skPlaneId.IntegerValue > 0) ? $"{skPlaneId.IntegerValue}" : "<none>";
    var planeName = skPlaneId != null && skPlaneId.IntegerValue > 0
                      ? (Doc.GetElement(skPlaneId) as DB.SketchPlane)?.Name
                      : "<none>";
    if (familyInstance.Location is DB.LocationPoint finalLp)
      DebugLog($"[RVTIN] Final location INTERNAL: {finalLp.Point}");
    DebugLog($"[RVTIN] Final: host:{(hostId.HasValue ? hostId.ToString() : "<null>")} SKETCH_PLANE_PARAM:{skPlaneStr} name:{planeName}");

    SetInstanceParameters(familyInstance, instance);
    var state = isUpdate ? ApplicationObject.State.Updated : ApplicationObject.State.Created;
    appObj.Update(status: state, createdId: familyInstance.UniqueId, convertedItem: familyInstance);
    return appObj;
  }











  private static bool TryGetDynBool(Base b, string key, out bool value)
  {
    value = false;
    if (b == null) return false;
    if (!b.GetMembers(Speckle.Core.Models.DynamicBaseMemberType.All).TryGetValue(key, out var raw) || raw == null)
      return false;

    switch (raw)
    {
      case bool bb: value = bb; return true;
      case string s when bool.TryParse(s, out var bv): value = bv; return true;
      case string s2 when int.TryParse(s2, out var iv): value = iv != 0; return true;
      case int i: value = i != 0; return true;
      case long l: value = l != 0; return true;
      case double d: value = Math.Abs(d) > 1e-9; return true;
      default: return false;
    }
  }

  /// Read from typed prop if present, otherwise from dynamic.
  /// Works even if your Speckle.Objects in Revit doesn’t have the new property.
  private static bool WantsVerticalFlip(Base b)
  {
    bool typed = false, hasTyped = false;
    if (b is Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance rwpi)
    {
      // If your local kit defines the property, this compiles and returns the value.
      typed = rwpi.flipVertical;
      hasTyped = true;
    }

    // Prefer dynamic if it exists (the viewer proves it does).
    if (TryGetDynBool(b, "flipVertical", out var dyn))
      return dyn;

    // Fall back to typed (or false if not present).
    return hasTyped && typed;
  }
  private enum AxisKind
  {
    WorldZ,     // yaw in plan
    WorldX,     // tilt around world X
    WorldY,     // tilt around world Y
    InPlaneX,   // axis = work-plane XVec (good for vertical tilt)
    InPlaneY,   // axis = work-plane YVec (good for vertical tilt)
    InstanceZ   // axis = fi local BasisZ (roll around its up)
  }

  private bool TryRotate90(Document doc, DB.FamilyInstance fi, AxisKind axisKind, Action<string> log)
  {
    if (fi == null) return false;

    // pivot: insertion point if we have one, else element center
    XYZ pivot = (fi.Location is DB.LocationPoint lp) ? lp.Point :
                ((fi.get_BoundingBox(null) is BoundingBoxXYZ bb) ? (bb.Min + bb.Max) * 0.5 : XYZ.Zero);

    // resolve axis direction
    XYZ axisDir;
    switch (axisKind)
    {
      case AxisKind.WorldZ: axisDir = DB.XYZ.BasisZ; break;
      case AxisKind.WorldX: axisDir = DB.XYZ.BasisX; break;
      case AxisKind.WorldY: axisDir = DB.XYZ.BasisY; break;
      case AxisKind.InstanceZ:
        axisDir = fi.GetTotalTransform().BasisZ;
        if (axisDir == null || axisDir.IsZeroLength()) axisDir = DB.XYZ.BasisZ;
        break;
      case AxisKind.InPlaneX:
      case AxisKind.InPlaneY:
      default:
        {
          // Try sketch plane first
          var spId = fi.get_Parameter(DB.BuiltInParameter.SKETCH_PLANE_PARAM)?.AsElementId();
          var sp = (spId != null && spId.IntegerValue > 0) ? doc.GetElement(spId) as DB.SketchPlane : null;
          if (sp != null)
          {
            var pl = sp.GetPlane();
            axisDir = (axisKind == AxisKind.InPlaneX ? pl.XVec : pl.YVec);
          }
          else
          {
            // Fallback to instance local bases
            var t = fi.GetTotalTransform();
            axisDir = (axisKind == AxisKind.InPlaneX ? t.BasisX : t.BasisY);
          }
          if (axisDir == null || axisDir.IsZeroLength()) axisDir = DB.XYZ.BasisX;
        }
        break;
    }
    axisDir = axisDir.Normalize();

    using var st = new DB.SubTransaction(doc);
    st.Start();

    // unpin if needed
    bool rePin = false;
    try { if (fi.Pinned) { fi.Pinned = false; rePin = true; } }
    catch (Exception ex) { log($"[R90] Unpin failed: {ex.Message}"); }

    try
    {
      var axis = DB.Line.CreateUnbound(pivot, axisDir);
      DB.ElementTransformUtils.RotateElement(doc, fi.Id, axis, Math.PI / 2.0); // 90°
      log("[R90] RotateElement 90° succeeded.");
    }
    catch (Exception ex)
    {
      log($"[R90] RotateElement failed: {ex.GetType().Name}: {ex.Message}");
      try { if (rePin) fi.Pinned = true; } catch { }
      st.RollBack();
      return false;
    }

    // Some hosted connection families lose void cut after transforms—reapply if possible
    try
    {
      var host = fi.Host as DB.Element;
      if (host != null)
      {
        DB.InstanceVoidCutUtils.AddInstanceVoidCut(doc, host, fi);
        log("[R90] AddInstanceVoidCut attempted.");
      }
      else log("[R90] No host to cut.");
    }
    catch (Exception ex) { log($"[R90] AddInstanceVoidCut failed: {ex.Message}"); }

    // re-pin
    try { if (rePin) fi.Pinned = true; }
    catch (Exception ex) { log($"[R90] Re-pin failed: {ex.Message}"); }

    st.Commit();
    return true;
  }


  private void RotateAsVerticalFlip(DB.FamilyInstance fi, XYZ pivot, bool usePlaneX = true)
  {
    // Try to use the instance's sketch plane orientation
    XYZ dir = null;

    var skId = fi.get_Parameter(DB.BuiltInParameter.SKETCH_PLANE_PARAM)?.AsElementId();
    var sp = (skId != null && skId.IntegerValue > 0) ? Doc.GetElement(skId) as DB.SketchPlane : null;

    if (sp != null)
    {
      var pl = sp.GetPlane();
      // axis in the host plane: XVec or YVec
      dir = (usePlaneX ? pl.XVec : pl.YVec);
    }

    // Fallback: use the instance’s local bases (still in model coords)
    if (dir == null || dir.IsZeroLength())
    {
      var t = fi.GetTotalTransform();
      dir = (usePlaneX ? t.BasisX : t.BasisY);
    }

    // Final fallback: world X
    if (dir == null || dir.IsZeroLength()) dir = DB.XYZ.BasisX;

    var axis = DB.Line.CreateUnbound(pivot, dir.Normalize());

    using (var st = new DB.SubTransaction(Doc))
    {
      st.Start();
      DB.ElementTransformUtils.RotateElement(Doc, fi.Id, axis, Math.PI); // 180°
      st.Commit();
    }
  }




  // old
  /*
  public ApplicationObject RevitInstanceToNative(Objects.Other.Revit.RevitInstance instance, ApplicationObject appObj = null)
  {
    DB.FamilyInstance familyInstance = null;
    var docObj = GetExistingElementByApplicationId(instance?.applicationId);
    appObj ??= new ApplicationObject(instance?.id, instance?.speckle_type) { applicationId = instance?.applicationId };
    var isUpdate = false;

    if (instance == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed, logItem: "Instance was null.");
      return appObj;
    }

    if (IsIgnore(docObj, appObj))
      return appObj;

    // Resolve FamilySymbol (typedDefinition/definition/symbol or fallback by names)
    var def =
        instance.typedDefinition as RevitSymbolElementType
        ?? instance.definition as RevitSymbolElementType
        ?? instance["symbol"] as RevitSymbolElementType;

    DB.FamilySymbol familySymbol = null;
    bool isExactMatch = false;

    if (def != null)
      familySymbol = GetElementType<DB.FamilySymbol>(def, appObj, out isExactMatch);

    if (familySymbol == null)
    {
      var fam = instance["family"] as string;
      var typ = instance["type"] as string;
      if (!string.IsNullOrWhiteSpace(fam) && !string.IsNullOrWhiteSpace(typ))
        familySymbol = FindFamilySymbolByName(fam, typ);
    }

    if (familySymbol == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed,
                    logItem: $"Family/type not found. family='{instance["family"] ?? "<null>"}' type='{instance["type"] ?? "<null>"}'.");
      return appObj;
    }

    if (!familySymbol.IsActive)
      familySymbol.Activate();

    // Level
    DB.Level level = ConvertLevelToRevit(instance.level, out ApplicationObject.State _);
    if (level == null)
    {
      level = FallbackLevel(instance["levelUniqueId"] as string) ?? CreateLevelIfNone();
      if (level == null)
      {
        appObj.Update(status: ApplicationObject.State.Failed, logItem: "No Level found or could be created.");
        return appObj;
      }
    }

    // Placement type
    string placementStr = (def?.placementType ?? instance["placementType"] as string) ?? string.Empty;
    var placement = Enum.TryParse<DB.FamilyPlacementType>(placementStr, true, out var placementType)
                      ? placementType
                      : familySymbol.Family?.FamilyPlacementType ?? DB.FamilyPlacementType.Invalid;

    // --- special-case: WorkPlane connection ---
    bool isRwpi = instance is Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance;
    var rwpi = isRwpi ? (Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance)instance : null;

    // Look up the host column by matching connection.sketchPlaneUniqueId -> column.applicationId
    Element explicitHost = null;
    if (isRwpi && !string.IsNullOrWhiteSpace(rwpi.sketchPlaneUniqueId))
    {
      explicitHost = FindHostColumnByApplicationId(rwpi.sketchPlaneUniqueId);
      if (explicitHost == null)
        appObj.Update(logItem: $"Host column with applicationId='{rwpi.sketchPlaneUniqueId}' not found.");
    }

    // Insertion point
    XYZ insertionPoint;
    if (isRwpi && explicitHost != null)
    {
      // Ignore incoming placement: use host center as neutral reference
      insertionPoint = ElementCenter(explicitHost);
    }
    else
    {
      if (instance["placementPoint"] is Objects.Geometry.Point pp)
      {
        var u = UnitsOrModel(pp.units);
        insertionPoint = new XYZ(
          ScaleToNative(pp.x, u),
          ScaleToNative(pp.y, u),
          ScaleToNative(pp.z, u)
        );
      }
      else if (instance.transform != null)
      {
        var t = TransformToNative(instance.transform);
        insertionPoint = t.OfPoint(XYZ.Zero);
      }
      else
      {
        insertionPoint = XYZ.Zero;
      }
    }

    // Update existing
    if (docObj != null)
    {
      try
      {
        var revitType = Doc.GetElement(docObj.GetTypeId()) as ElementType;

        if (revitType == null || familySymbol.FamilyName != revitType.FamilyName)
        {
          Doc.Delete(docObj.Id);
        }
        else
        {
          familyInstance = (DB.FamilyInstance)docObj;

          if (isRwpi)
          {
            // Do NOT touch position/rotation. Ensure host matches; if not, recreate.
            if (explicitHost != null && familyInstance.Host?.Id != explicitHost.Id)
            {
              Doc.Delete(docObj.Id);
              familyInstance = null;
            }
            else
            {
              // Allow type changes
              if (isExactMatch && revitType.Id.IntegerValue != familySymbol.Id.IntegerValue)
                familyInstance.ChangeTypeId(familySymbol.Id);
            }
          }
          else
          {
            // Your existing update behavior (position/level/type)
            var newPt = new XYZ(insertionPoint.X, insertionPoint.Y, (familyInstance.Location as LocationPoint).Point.Z);
            (familyInstance.Location as LocationPoint).Point = newPt;

            if ((familyInstance.Location as LocationPoint).Point != newPt)
              (familyInstance.Location as LocationPoint).Point = newPt;

            if (isExactMatch && revitType.Id.IntegerValue != familySymbol.Id.IntegerValue)
              familyInstance.ChangeTypeId(familySymbol.Id);

            TrySetParam(familyInstance, BuiltInParameter.FAMILY_LEVEL_PARAM, level);
            TrySetParam(familyInstance, BuiltInParameter.FAMILY_BASE_LEVEL_PARAM, level);
          }
        }
        isUpdate = familyInstance != null;
      }
      catch (Autodesk.Revit.Exceptions.ApplicationException)
      {
        // fall through to re-create
      }
    }

    // Create new
    if (familyInstance == null)
    {
      if (isRwpi && explicitHost != null && placement == DB.FamilyPlacementType.WorkPlaneBased)
      {
        // Host by face on the column; neutral orientation (no rotation)
        var faceRef = GetNearestPlanarFaceReference(explicitHost, insertionPoint);

        if (faceRef != null)
        {
          familyInstance = Doc.Create.NewFamilyInstance(
            faceRef, insertionPoint, new XYZ(0, 0, 0), familySymbol);
        }
        else
        {
          // Fallback: try generic host-based creation (might throw if not supported)
          try
          {
            familyInstance = Doc.Create.NewFamilyInstance(
              insertionPoint, familySymbol, explicitHost, level, DB.Structure.StructuralType.NonStructural);
          }
          catch
          {
            appObj.Update(status: ApplicationObject.State.Failed,
                          logItem: "Could not obtain a face reference for WorkPlane hosting and fallback creation failed.");
            return appObj;
          }
        }

        // Optional: set "Schedule Level" if present
        var lvlParams = familyInstance.GetParameters("Schedule Level");
        if (lvlParams?.Count > 0 && level != null)
          lvlParams[0].Set(level.Id);
      }
      else
      {
        switch (placement)
        {
          case DB.FamilyPlacementType.OneLevelBasedHosted when CurrentHostElement != null:
            familyInstance = Doc.Create.NewFamilyInstance(
              insertionPoint, familySymbol, CurrentHostElement, level, DB.Structure.StructuralType.NonStructural);
            break;

          case DB.FamilyPlacementType.WorkPlaneBased when CurrentHostElement != null:
            {
              var op = new Options { ComputeReferences = true };
              var geomElement = CurrentHostElement.get_Geometry(op);
              if (geomElement == null)
              {
                Doc.Regenerate();
                geomElement = CurrentHostElement.get_Geometry(op);
              }
              if (geomElement == null) goto default;

              Reference faceRef = null;
              var planeDist = double.MaxValue;
              GetReferencePlane(geomElement, insertionPoint, ref faceRef, ref planeDist);

              familyInstance = Doc.Create.NewFamilyInstance(faceRef, insertionPoint, new XYZ(0, 0, 0), familySymbol);

              var lvlParams = familyInstance.GetParameters("Schedule Level");
              if (lvlParams?.Count > 0 && level != null) lvlParams[0].Set(level.Id);

              break;
            }

          case DB.FamilyPlacementType.WorkPlaneBased:
            {
              SketchPlane sp = null;
              if (instance is Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance wpi)
              {
                if (!string.IsNullOrWhiteSpace(wpi.sketchPlaneUniqueId))
                  sp = Doc.GetElement(wpi.sketchPlaneUniqueId) as SketchPlane;

                if (sp == null && wpi.workPlane != null && wpi.workPlane.origin != null && wpi.workPlane.normal != null)
                {
                  var ou = UnitsOrModel(wpi.workPlane.origin.units);
                  var n = new XYZ(wpi.workPlane.normal.x, wpi.workPlane.normal.y, wpi.workPlane.normal.z);
                  var o = new XYZ(
                    ScaleToNative(wpi.workPlane.origin.x, ou),
                    ScaleToNative(wpi.workPlane.origin.y, ou),
                    ScaleToNative(wpi.workPlane.origin.z, ou));
                  var plane = DB.Plane.CreateByNormalAndOrigin(n, o);
                  sp = SketchPlane.Create(Doc, plane);
                }
              }

              familyInstance = (sp != null)
                ? Doc.Create.NewFamilyInstance(insertionPoint, familySymbol, sp, DB.Structure.StructuralType.NonStructural)
                : Doc.Create.NewFamilyInstance(insertionPoint, familySymbol, level, DB.Structure.StructuralType.NonStructural);
              break;
            }

          default:
            familyInstance = Doc.Create.NewFamilyInstance(
              insertionPoint, familySymbol, level, DB.Structure.StructuralType.NonStructural);
            break;
        }
      }
    }

    if (familyInstance == null)
    {
      appObj.Update(status: ApplicationObject.State.Failed, logItem: "Could not create instance.");
      return appObj;
    }

    Doc.Regenerate(); // required for mirror/flip/rotation to behave

    if (!isRwpi)
    {
      // Mirror
      if (instance.mirrored != familyInstance.Mirrored)
      {
        Group group = null;
        try { group = CurrentHostElement != null ? Doc.Create.NewGroup(new[] { familyInstance.Id }) : null; }
        catch (Autodesk.Revit.Exceptions.InvalidOperationException) { }

        var toMirror = group != null ? new[] { group.Id } : new[] { familyInstance.Id };
        try
        {
          ElementTransformUtils.MirrorElements(
            Doc, toMirror, DB.Plane.CreateByNormalAndOrigin(XYZ.BasisY, insertionPoint), false);
        }
        catch (Autodesk.Revit.Exceptions.ApplicationException e)
        {
          appObj.Update(logItem: $"Instance could not be mirrored: {e.Message}");
        }
        group?.UngroupMembers();
      }

      // Flip
      if (familyInstance.CanFlipHand && instance.handFlipped != familyInstance.HandFlipped)
        familyInstance.flipHand();
      if (familyInstance.CanFlipFacing && instance.facingFlipped != familyInstance.FacingFlipped)
        familyInstance.flipFacing();

      // Rotation
      if (instance["rotation"] is double rot && Math.Abs(rot) > 1e-9 && familyInstance.Location is LocationPoint lp1)
      {
        var axis = DB.Line.CreateUnbound(lp1.Point, XYZ.BasisZ);
        try { ElementTransformUtils.RotateElement(Doc, familyInstance.Id, axis, rot); }
        catch (Autodesk.Revit.Exceptions.ApplicationException e)
        { appObj.Update(logItem: $"Could not rotate instance: {e.Message}"); }
      }
      else if (instance.transform != null && familyInstance.Location is LocationPoint lp2)
      {
        var desired = TransformToNative(instance.transform);
        var current = familyInstance.GetTotalTransform();
        XYZ dX = desired.BasisX, cX = current.BasisX, cZ = current.BasisZ;
        var cross = cX.CrossProduct(dX);
        var dot = cX.DotProduct(dX);
        var rotZ = Math.Atan2(cross.DotProduct(cZ), dot);
        if (Math.Abs(rotZ) > 1e-9)
        {
          var axis = DB.Line.CreateUnbound(lp2.Point, cZ);
          try { ElementTransformUtils.RotateElement(Doc, familyInstance.Id, axis, -rotZ); }
          catch (Autodesk.Revit.Exceptions.ApplicationException e)
          { appObj.Update(logItem: $"Could not rotate created instance: {e.Message}"); }
        }
      }
    }
    // else: RWPI → intentionally do nothing about position or rotation

    SetInstanceParameters(familyInstance, instance);
    var state = isUpdate ? ApplicationObject.State.Updated : ApplicationObject.State.Created;
    appObj.Update(status: state, createdId: familyInstance.UniqueId, convertedItem: familyInstance);
    return appObj;
  }

*/




  private string UnitsOrModel(string units)
  {
    return string.IsNullOrWhiteSpace(units) || units.Equals("none", StringComparison.OrdinalIgnoreCase)
      ? ModelUnits
      : units;
  }





















































  // Find a FamilySymbol by family & type name (case-insensitive).
  private FamilySymbol FindFamilySymbolByName(string family, string type)
  {
    return new FilteredElementCollector(Doc)
      .OfClass(typeof(FamilySymbol)).Cast<FamilySymbol>()
      .FirstOrDefault(fs =>
        fs.Family?.Name?.Equals(family, StringComparison.OrdinalIgnoreCase) == true &&
        fs.Name?.Equals(type, StringComparison.OrdinalIgnoreCase) == true);
  }

  // Try by UID, otherwise first level by elevation.
  private Level FallbackLevel(string levelUniqueId = null)
  {
    if (!string.IsNullOrWhiteSpace(levelUniqueId))
    {
      var byUid = Doc.GetElement(levelUniqueId) as Level;
      if (byUid != null) return byUid;
    }

    return new FilteredElementCollector(Doc)
      .OfClass(typeof(Level)).Cast<Level>()
      .OrderBy(l => l.Elevation)
      .FirstOrDefault();
  }

  // Create a level @ 0 if the model has none (safe in Receive transaction).
  private Level CreateLevelIfNone()
  {
    var any = new FilteredElementCollector(Doc).OfClass(typeof(Level)).FirstElement() as Level;
    if (any != null) return any;
    return Level.Create(Doc, 0.0);
  }



  private string NormalizeUnits(string units)
  {
    return string.IsNullOrWhiteSpace(units) || units.Equals("none", StringComparison.OrdinalIgnoreCase)
      ? ModelUnits
      : units;
  }





















  public List<Base> ConvertToSpeckle(List<object> objects) => objects.Select(ConvertToSpeckle).ToList();

  public List<object> ConvertToNative(List<Base> objects)
  {
    if (objects == null || objects.Count == 0)
      return new List<object>();

    static bool IsDeferred(Base b)
    {
      if (b is Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance) return true;

      // Handle @SpeckleSchema wrapper
      var schema = b?["@SpeckleSchema"] as Base;
      return schema is Objects.BuiltElements.Revit.RevitWorkPlaneFamilyInstance;
    }

    var immediate = new List<Base>(objects.Count);
    var deferred = new List<Base>();

    foreach (var b in objects)
    {
      if (b == null) continue;
      if (IsDeferred(b)) deferred.Add(b);
      else immediate.Add(b);
    }

    var results = new List<object>(objects.Count);

    // Use lambdas to avoid overload ambiguity
    results.AddRange(immediate.Select(b => ConvertToNative(b)));
    results.AddRange(deferred.Select(b => ConvertToNative(b)));

    return results;
  }


  /// Rotate 180° around an in-plane axis through the given pivot point.
  private static void RotateInstance180InPlane(Document doc, DB.FamilyInstance fi, XYZ pivot, bool usePlaneX = true)
  {
    var dir = ResolveInPlaneAxis(fi, usePlaneX);
    var axis = DB.Line.CreateUnbound(pivot, dir);

    using (var st = new SubTransaction(doc))
    {
      st.Start();
      ElementTransformUtils.RotateElement(doc, fi.Id, axis, Math.PI); // 180 degrees
      st.Commit();
    }
  }
  private static SketchPlane TryGetSketchPlane(DB.FamilyInstance fi)
  {
    var id = fi.get_Parameter(BuiltInParameter.SKETCH_PLANE_PARAM)?.AsElementId();
    return (id != null && id.IntegerValue > 0) ? fi.Document.GetElement(id) as SketchPlane : null;
  }

  private static XYZ ResolveInPlaneAxis(DB.FamilyInstance fi, bool usePlaneX = true)
  {
    var sp = TryGetSketchPlane(fi);
    if (sp != null)
    {
      var pl = sp.GetPlane();
      var d = usePlaneX ? pl.XVec : pl.YVec;
      if (d != null && !d.IsZeroLength()) return d.Normalize();
    }
    var t = fi.GetTotalTransform();
    var alt = usePlaneX ? t.BasisX : t.BasisY;
    return (alt != null && !alt.IsZeroLength()) ? alt.Normalize() : XYZ.BasisX;
  }

  private static void VerticalFlipSimple(Document doc, DB.FamilyInstance fi, bool usePlaneX = true)
  {
    if (fi?.Location is not LocationPoint lp) return;

    // cache an editable offset param (work-plane based uses FREE_HOST_OFFSET, others use ELEVATION)
    DB.Parameter target = fi.get_Parameter(BuiltInParameter.INSTANCE_FREE_HOST_OFFSET_PARAM);
    if (target is null || target.IsReadOnly)
      target = fi.get_Parameter(BuiltInParameter.INSTANCE_ELEVATION_PARAM);

    double? oldOffset = (target != null) ? (double?)target.AsDouble() : null;

    var axis = DB.Line.CreateUnbound(lp.Point, ResolveInPlaneAxis(fi, usePlaneX));

    using (var st = new SubTransaction(doc))
    {
      st.Start();

      // 1) rotate 180°
      ElementTransformUtils.RotateElement(doc, fi.Id, axis, Math.PI);

      // 2) flip the offset sign so the part ends up on the visible side of the host
      if (oldOffset.HasValue && target is { IsReadOnly: false })
        target.Set(-oldOffset.Value);

      st.Commit();
    }
  }

  public bool CanConvertToSpeckle(object @object)
  {
    return @object switch
    {
      DB.DetailCurve _ => true,
      DB.Material _ => true,
      DB.DirectShape _ => true,
      DB.FamilyInstance _ => true,
      DB.Floor _ => true,
      DB.Level _ => true,
      DB.View _ => true,
      DB.ModelCurve _ => true,
      DB.Opening _ => true,
      DB.RoofBase _ => true,
      DB.Area _ => true,
      DB.Architecture.Room _ => true,
      DB.Architecture.TopographySurface _ => true,
      DB.Wall _ => true,
      DB.Mechanical.Duct _ => true,
      DB.Mechanical.FlexDuct _ => true,
      DB.Mechanical.Space _ => true,
      DB.Plumbing.Pipe _ => true,
      DB.Plumbing.FlexPipe _ => true,
      DB.Electrical.Wire _ => true,
      DB.Electrical.CableTray _ => true,
      DB.Electrical.Conduit _ => true,
      DB.CurtainGridLine _ => true, //these should be handled by curtain walls
      DB.Architecture.BuildingPad _ => true,
      DB.Architecture.Stairs _ => true,
      DB.Architecture.StairsRun _ => true,
      DB.Architecture.StairsLanding _ => true,
      DB.Architecture.Railing _ => true,
      DB.Architecture.TopRail _ => true,
      DB.Ceiling _ => true,
      DB.PointCloudInstance _ => true,
      DB.Group _ => true,
      DB.ProjectInfo _ => true,
      DB.ElementType _ => true,
      DB.Grid _ => true,
      DB.ReferencePoint _ => true,
      DB.FabricationPart _ => true,
      DB.CombinableElement _ => true,

#if (REVIT2024)
      DB.Toposolid _ => true,
#endif

#if REVIT2020 || REVIT2021 || REVIT2022
      DB.Structure.AnalyticalModelStick _ => true,
      DB.Structure.AnalyticalModelSurface _ => true,
#else
      DB.Structure.AnalyticalMember _ => true,
      DB.Structure.AnalyticalPanel _ => true,
#endif
      DB.Structure.BoundaryConditions _ => true,
      _ => (@object as Element).IsElementSupported()
    };
  }

  public bool CanConvertToNative(Base @object)
  {
    //Family Document
    if (Doc.IsFamilyDocument)
    {
      return @object switch
      {
        ICurve _ => true,
        Geometry.Brep _ => true,
        Geometry.Mesh _ => true,
        BER.FreeformElement _ => true,
        _ => false
      };
    }

    //Project Document
    var schema = @object["@SpeckleSchema"] as Base; // check for contained schema
    if (schema != null)
    {
      return CanConvertToNative(schema);
    }

    var objRes = @object switch
    {
      //geometry
      ICurve _ => true,
      Geometry.Brep _ => true,
      Geometry.Mesh _ => true,
      // non revit built elems
      BE.Structure _ => true,
      BE.Alignment _ => true,
      //built elems
      BER.AdaptiveComponent _ => true,
      BE.Beam _ => true,
      BE.Brace _ => true,
      BE.Column _ => true,
#if !REVIT2020 && !REVIT2021
      BE.Ceiling _ => true,
#endif
      BERC.DetailCurve _ => true,
      BER.DirectShape _ => true,
      BER.FreeformElement _ => true,
      BER.FamilyInstance _ => true,
      BE.Floor _ => true,
      BE.Level _ => true,
      BERC.ModelCurve _ => true,
      BE.Opening _ => true,
      BERC.RoomBoundaryLine _ => true,
      BERC.SpaceSeparationLine _ => true,
      BE.Roof _ => true,

#if (REVIT2024)
      RevitToposolid _ => true,
#endif
      BE.Topography _ => true,
      BER.RevitCurtainWallPanel _ => true,
      BER.RevitFaceWall _ => true,
      BER.RevitProfileWall _ => true,
      BE.Wall _ => true,
      BE.Duct _ => true,
      BE.Pipe _ => true,
      BE.Wire _ => true,
      BE.CableTray _ => true,
      BE.Conduit _ => true,
      BE.Revit.RevitRailing _ => true,
      Other.Revit.RevitInstance _ => true,
      BER.ParameterUpdater _ => true,
      BE.View3D _ => true,
      BE.Room _ => true,
      BE.GridLine _ => true,
      BE.Space _ => true,
      BE.Network _ => true,
      //Structural
      STR.Geometry.Element1D _ => true,
      STR.Geometry.Element2D _ => true,
      Other.BlockInstance _ => true,
      Other.MappedBlockWrapper => true,
      Organization.DataTable _ => true,
      // GIS
      PolygonElement _ => true,
      GisFeature feat
        when (
          feat.GetMembers(DynamicBaseMemberType.All).TryGetValue("displayValue", out var value)
          && value is List<Base> valueList
          && valueList.Count > 0
        )
        => true,
      _ => false,
    };
    if (objRes)
    {
      return true;
    }

    return false;
  }

  public bool CanConvertToNativeDisplayable(Base @object)
  {
    // check for schema
    var schema = @object["@SpeckleSchema"] as Base; // check for contained schema
    if (schema != null)
    {
      return CanConvertToNativeDisplayable(schema);
    }

    return @object.IsDisplayableObject();
  }
}
