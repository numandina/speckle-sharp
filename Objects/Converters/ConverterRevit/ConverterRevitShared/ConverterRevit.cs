
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

namespace Objects.Converter.Revit;

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




  private readonly List<Objects.BuiltElements.Revit.StructuralConnection> _pendingSC = new();
  private readonly HashSet<string> _pendingSCKeys = new();
  private bool _isFlushingSC = false;



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

  public ConverterRevit()
  {
    var ver = System.Reflection.Assembly.GetAssembly(typeof(ConverterRevit)).GetName().Version;
    Report.Log($"Using converter: {Name} v{ver}");

    try
    {
      var objAsm = typeof(Objects.Geometry.Mesh).Assembly; // from Objects.dll
      Report.Log($"[DEBUG] Objects.dll loaded from: {objAsm.Location} (v{objAsm.GetName().Version})");
    }
    catch (System.Exception ex)
    {
      Report.Log($"[DEBUG] Could not resolve Objects.dll path: {ex.Message}");
    }

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
      Report.Log($"Using document: {Doc.PathName}");
      Report.Log($"Using units: {ModelUnits}");
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

  public void SetConverterSettings(object settings)
  {
    Settings = settings as Dictionary<string, string>;
  }
  private static bool __bootPopupShown = false;

  public Base ConvertToSpeckle(object @object)
  {

    // ---- BOOT PROBE: prints once per send session ----
    try
    {
      if (!__bootPopupShown)
      {
        __bootPopupShown = true;

        var objAsm = typeof(Objects.Geometry.Mesh).Assembly;
        var convAsm = typeof(ConverterRevit).Assembly;
        var msg =
          "[SPECKLE DEBUG]\n" +
          $"Objects.dll:   {objAsm.Location}\n" +
          $"Objects ver:   {objAsm.GetName().Version}\n" +
          $"Converter dll: {convAsm.Location}\n" +
          $"Converter ver: {convAsm.GetName().Version}";

        // 1) Try a Revit TaskDialog via reflection (no compile-time dependency)
        try
        {
          var tdType = Type.GetType("Autodesk.Revit.UI.TaskDialog, RevitAPIUI");
          var show = tdType?.GetMethod("Show", new[] { typeof(string), typeof(string) });
          show?.Invoke(null, new object[] { "Speckle Converter Debug", msg });
        }
        catch { /* swallow */ }

        // 2) Also file-log (so we have breadcrumbs even if UI is suppressed)
        try
        {
          var logPath = System.IO.Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "Speckle", "Logs", "converter-boot.log"
          );
          System.IO.Directory.CreateDirectory(System.IO.Path.GetDirectoryName(logPath)!);
          System.IO.File.AppendAllText(logPath, msg + Environment.NewLine);
        }
        catch { /* swallow */ }

        // 3) And write to Speckle panel
        try { Report.Log(msg); } catch { /* swallow */ }
      }
    }
    catch { /* ignore */ }




    Base returnObject = null;
    List<string> notes = new();
    string id = @object is Element element ? element.UniqueId : string.Empty;

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
      case DB.FamilyInstance o:
        {
          // SPECIAL CASE: Structural Connections families (category = OST_StructConnections)
          var cat = o.Category;
          if (cat != null && (DB.BuiltInCategory)cat.Id.IntegerValue == DB.BuiltInCategory.OST_StructConnections)
          {
            Report.Log($"[SC-HIT-FI] Treating FamilyInstance {o.UniqueId} (category OST_StructConnections) as StructuralConnection.");
            returnObject = FamilyInstanceStructuralConnectionToSpeckle(o);
            break;
          }

          // default family path
          returnObject = FamilyInstanceToSpeckle(o, out notes);
          break;
        }
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
        Report.Log($"[SC-HIT] Converting StructuralConnectionHandler: {o.UniqueId} ({o.GetType().FullName})");
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
        if (el != null && el.Category != null &&
            (BuiltInCategory)el.Category.Id.IntegerValue == BuiltInCategory.OST_StructConnections)
        {
          Report.Log($"[SC-MISS] Fell through to RevitInstance for {el.UniqueId} (category = OST_StructConnections, class = {el.GetType().FullName})");
        }

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

  private Base FamilyInstanceStructuralConnectionToSpeckle(DB.FamilyInstance fi)
  {
    // Build a minimal StructuralConnection Speckle object from a FamilyInstance in the Structural Connections category.
    var sc = new Objects.BuiltElements.Revit.StructuralConnection
    {
      applicationId = fi.UniqueId,
      typeId = fi.Symbol?.Id.IntegerValue.ToString(),
      typeName = fi.Symbol?.Name
    };

    // Best-effort: harvest a mesh for viewers/Unity
    try { sc.displayValue = GetElementDisplayValue(fi); } catch { }

    // Keep Revit params/ids for round-tripping context
    try { GetAllRevitParamsAndIds(sc, fi); } catch { }

    // Debug probe so you can see it in the Speckle viewer
    sc["probe"] = "SC from FamilyInstance";

    return sc;
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

  // helper: find elements by our invented appId
  private IReadOnlyList<ElementId> FindByAppId(string appId)
  {
    if (string.IsNullOrWhiteSpace(appId)) return Array.Empty<ElementId>();

    if (_appIdIndex.TryGetValue(appId, out var ids) && ids != null && ids.Count > 0)
    {
      //SC_LOG($"  [SC][RESOLVE] appId '{appId}' -> [{string.Join(",", ids.Select(i => i.IntegerValue))}] (INDEX)");
      return ids;
    }

    // fallback only to PARAM lookup (still by applicationId)
    try
    {
      var el = new FilteredElementCollector(Doc)
        .WhereElementIsNotElementType()
        .Cast<Element>()
        .FirstOrDefault(e =>
        {
          var p = e.LookupParameter("Speckle.ApplicationId");
          return p != null && string.Equals(p.AsString(), appId, StringComparison.OrdinalIgnoreCase);
        });

      if (el != null)
      {
        //SC_LOG($"  [SC][RESOLVE] appId '{appId}' -> {el.Id.IntegerValue} (PARAM)");
        // also push it into the index so next time it's instant
        if (!_appIdIndex.TryGetValue(appId, out var list))
          _appIdIndex[appId] = list = new List<ElementId>();
        if (!list.Contains(el.Id)) list.Add(el.Id);
        return new[] { el.Id };
      }
    }
    catch { /* ignore */ }

  //  SC_LOG($"  [SC][RESOLVE] appId '{appId}' -> MISS");
    return Array.Empty<ElementId>();
  }


  // ensure this is called after *every* successful conversion
  public object ConvertToNative(Base @base)
  {
    var nativeObject = ConvertToNativeObject(@base);

    switch (nativeObject)
    {
      case ApplicationObject appObject:
        if (appObject.Converted.Cast<Element>().ToList() is List<Element> elems && elems.Count >= 1)
        {
          receivedObjectsCache?.AddConvertedObjects(@base, elems);
          foreach (var el in elems) IndexAppIdFor(@base, el); // <— important
        }
        break;

      case Element single:
        receivedObjectsCache?.AddConvertedObjects(@base, new List<Element> { single });
        IndexAppIdFor(@base, single); // <— important
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

      case BER.StructuralConnection o:
        return StructuralConnectionToNative(o);

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

  public object ConvertToNativeDisplayable(Base @base)
  {
    var nativeObject = DisplayableObjectToNative(@base);
    if (nativeObject.Converted.Cast<Element>().ToList() is List<Element> typedList && typedList.Count >= 1)
    {
      receivedObjectsCache?.AddConvertedObjects(@base, typedList);
    }
    return nativeObject;
  }

  public List<Base> ConvertToSpeckle(List<object> objects) => objects.Select(ConvertToSpeckle).ToList();

  public List<object> ConvertToNative(List<Base> objects)
  {
    var results = new List<object>(objects.Count);

    foreach (var b in objects)
      results.Add(ConvertToNative(b)); // first pass

    // second pass for deferred StructuralConnections
    if (_pendingSC.Count > 0)
    {
      _isFlushingSC = true;
      try
      {
        var pendingKeys = _pendingSC
          .Select(x => string.Join("|", x.connectedElementUniqueIds ?? Enumerable.Empty<string>()))
          .ToList();

       // SC_LOG($"[SC][FLUSH][BEGIN] pending={_pendingSC.Count}");
      //  SC_LOG($"[SC][FLUSH] pending host-keys:\n  - {string.Join("\n  - ", pendingKeys)}");
      //  SC_LOG($"[SC][FLUSH] index keys:\n  - {string.Join("\n  - ", _appIdIndex.Keys)}");

        var pendings = _pendingSC.ToList(); // snapshot
        _pendingSC.Clear();
        _pendingSCKeys.Clear();

        int processed = 0;
        foreach (var sc in pendings)
        {
       //   SC_LOG($"[SC][FLUSH][TRY] appId='{sc.applicationId}' hosts={string.Join(",", sc.connectedElementUniqueIds ?? Enumerable.Empty<string>())}");
          var res = ConvertToNative(sc);   // re-run with hosts now in the model
          results.Add(res);
          processed++;
        }

      //  SC_LOG($"[SC][FLUSH][END] processed={processed}");
      }
      finally
      {
        _isFlushingSC = false;
      }
    }
    else
    {
      //SC_LOG("[SC][FLUSH] skipped (no pending)");
    }

    return results;
  }


  private T WithWriteTx<T>(string name, Func<T> body)
  {
    if (Doc.IsModifiable)
    {
     // SC_LOG($"[TX] SubTransaction: {name}");
      using (var st = new SubTransaction(Doc))
      {
        st.Start();
        var result = body();
        st.Commit();
        return result;
      }
    }
    else
    {
   //   SC_LOG($"[TX] Transaction: {name}");
      using (var tx = new Transaction(Doc, name))
      {
        tx.Start();
        var result = body();
        tx.Commit();
        return result;
      }
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
      DB.Structure.StructuralConnectionHandler _ => true,


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


  // Map of our invented appIds -> created Revit ElementIds in this receive pass
  private readonly Dictionary<string, List<ElementId>> _appIdIndex =
    new Dictionary<string, List<ElementId>>(StringComparer.OrdinalIgnoreCase);

  private void IndexConverted(string appId, IEnumerable<Element> elems)
  {
    if (string.IsNullOrWhiteSpace(appId) || elems == null) return;
    var ids = elems.Where(e => e != null).Select(e => e.Id).ToList();
    if (ids.Count == 0) return;

    _appIdIndex[appId] = ids;
    //SC_LOG($"[IDX][ADD] {appId} -> [{string.Join(",", ids.Select(i => i.IntegerValue))}]");
  }


  // add/replace this helper
  private void IndexAppIdFor(Base speckle, Element el)
  {
    var key = speckle?.applicationId ?? speckle?["applicationId"] as string;
    if (string.IsNullOrWhiteSpace(key))
    {
     // SC_LOG($"[IDX][SKIP] no appId for {el.Id.IntegerValue} ({el.GetType().Name})");
      return;
    }

    // stamp the parameter so we can always find by appId later
    try
    {
      var p = el.LookupParameter("Speckle.ApplicationId");
      if (p != null && !p.IsReadOnly)
      {
        var prev = p.AsString();
        if (!string.Equals(prev, key, StringComparison.OrdinalIgnoreCase))
        {
          p.Set(key);
        //  SC_LOG($"[IDX][STAMP] {el.Id.IntegerValue} param=Speckle.ApplicationId set to '{key}' (was '{prev}')");
        }
      }
    }
    catch { /* ignore */ }

    if (!_appIdIndex.TryGetValue(key, out var list))
      _appIdIndex[key] = list = new List<ElementId>();

    if (!list.Contains(el.Id)) list.Add(el.Id);

   // SC_LOG($"[IDX][ADD] {key} -> [{string.Join(",", list.Select(x => x.IntegerValue))}] ({el.GetType().Name})");
    TryResolvePendingForKey(key);   // <— add this line

  }



  // ---- the missing helper ----
  private void TryResolvePendingForKey(string key)
  {
    if (_pendingSC.Count == 0) return;

    // Find pending SCs that include this key AND now have all hosts available in the index
    var ready = new List<Objects.BuiltElements.Revit.StructuralConnection>();
   // SC_LOG($"[SC][FLUSH] starting, pending={_pendingSC.Count}, indexKeys={_appIdIndex.Count}");

    foreach (var sc in _pendingSC)
    {
      var keys = sc.connectedElementUniqueIds ?? Enumerable.Empty<string>();
      if (!keys.Contains(key)) continue;

      bool allReady = keys.All(k =>
        !string.IsNullOrWhiteSpace(k) &&
        _appIdIndex.TryGetValue(k, out var ids) &&
        ids != null && ids.Count > 0);

      if (allReady) ready.Add(sc);
    }

    if (ready.Count == 0) return;

   // SC_LOG($"[SC][RESOLVE] key '{key}' unlocked {ready.Count} pending connection(s)");

    // Remove from pending before attempting to create
    foreach (var sc in ready)
    {
      var k = sc.applicationId ?? sc.id ?? Guid.NewGuid().ToString("N");
      _pendingSC.Remove(sc);
      _pendingSCKeys.Remove(k);
    }

    // Build them now (mark as flushing so we don't re-defer)
    var wasFlushing = _isFlushingSC;
    _isFlushingSC = true;
    try
    {
      foreach (var sc in ready)
        StructuralConnectionToNative(sc);
    }
    finally
    {
      _isFlushingSC = wasFlushing;
    }
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
      BER.StructuralConnection _ => true,

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
