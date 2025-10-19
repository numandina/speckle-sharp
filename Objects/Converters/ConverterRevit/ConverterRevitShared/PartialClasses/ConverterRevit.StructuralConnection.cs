// Objects.Converter.Revit/ConverterRevit.StructuralConnection.cs
using System;
using System.Collections.Generic;
using System.Linq;
using Autodesk.Revit.DB;
using Autodesk.Revit.DB.Structure;
using Objects.BuiltElements.Revit;
using Speckle.Core.Models;
using DB = Autodesk.Revit.DB;
using STR = Autodesk.Revit.DB.Structure;
using BER = Objects.BuiltElements.Revit;
using Base = Speckle.Core.Models.Base;
using SPoint = Objects.Geometry.Point;

namespace Objects.Converter.Revit
{
  public partial class ConverterRevit
  {
    // ----------------------------
    // Revit ➜ Speckle (HANDLER)
    // ----------------------------
    public BER.StructuralConnection StructuralConnectionToSpeckle(STR.StructuralConnectionHandler h)
    {
      var doc = h.Document;
      var type = doc.GetElement(h.GetTypeId()) as STR.StructuralConnectionHandlerType;
      var et = type as ElementType;

      var sc = new BER.StructuralConnection
      {
        applicationId = h.UniqueId,
        family = et?.FamilyName,     // may be null for some handler types; that's fine
        type = type?.Name,
        typeId = h.GetTypeId()?.IntegerValue.ToString(),
        typeName = type?.Name,
        approvalTypeId = h.ApprovalTypeId?.IntegerValue.ToString(),
        codeCheckingStatus = h.CodeCheckingStatus.ToString(),
        builtInCategory = "OST_StructConnections"
      };

      // basePoint (best-effort; some Revit versions expose GetOrigin)
      try
      {
        var mi = h.GetType().GetMethod("GetOrigin");
        var xyz = mi?.Invoke(h, null) as DB.XYZ;
        if (xyz != null)
          sc.basePoint = new Objects.Geometry.Point { x = xyz.X, y = xyz.Y, z = xyz.Z, units = ModelUnits };
      }
      catch { /* ignore */ }

      // Connected hosts: fill BOTH the new detached list and the legacy UniqueIds
      foreach (var id in h.GetConnectedElementIds())
      {
        if (doc.GetElement(id) is DB.Element el)
        {
          sc.connectedElementUniqueIds.Add(el.UniqueId);               // legacy
          var host = RevitElementToSpeckle(el, out _);                 // minimal wrapper
          if (host != null) sc.connectedElements.Add(host);            // detached
        }
      }

      // Display meshes (detached)
      sc.displayValue = CoerceDisplayToMeshList(GetElementDisplayValue(h, GetFine3DOptions()));

      GetAllRevitParamsAndIds(sc, h);
      return sc;
    }

    private List<Objects.Geometry.Mesh> CoerceDisplayToMeshList(object dv)
    {
      if (dv == null) return new();
      if (dv is List<Objects.Geometry.Mesh> lm) return lm;
      if (dv is IEnumerable<Objects.Geometry.Mesh> em) return em.ToList();
      if (dv is IEnumerable<Base> eb) return eb.OfType<Objects.Geometry.Mesh>().ToList();
      if (dv is Objects.Geometry.Mesh m) return new() { m };
      return new();
    }















    // -----------------------------------------------
    // Revit ➜ Speckle (FamilyInstance in OST_StructConnections)
    // -----------------------------------------------
    // -----------------------------------------------
    // Revit ➜ Speckle (FamilyInstance in OST_StructConnections)
    // -----------------------------------------------

    public BER.StructuralConnection StructuralConnectionFamilyInstanceToSpeckle(DB.FamilyInstance fi)
    {
      Dbg.W($"[SC-FI] ENTER uid={fi?.UniqueId ?? "<null>"} id={(fi != null ? fi.Id.IntegerValue.ToString() : "<null>")} type={fi?.GetType()?.FullName ?? "<null>"}");
      Dbg.W($"[SC-FI] symbol? {(fi?.Symbol == null ? "<null>" : fi.Symbol.GetType().FullName)} fam={fi?.Symbol?.FamilyName} type={fi?.Symbol?.Name}");
      Dbg.W($"[SC-FI] cat={(BuiltInCategory?)fi?.Category?.Id.IntegerValue} symCat={(BuiltInCategory?)fi?.Symbol?.Category?.Id.IntegerValue}");

      var sc = new BER.StructuralConnection
      {
        applicationId = fi.UniqueId,
        family = fi.Symbol?.FamilyName,
        type = fi.Symbol?.Name,
        typeId = fi.GetTypeId()?.IntegerValue.ToString(),
        typeName = fi.Symbol?.Name,
        builtInCategory = "OST_StructConnections"
      };
      Dbg.W($"[SC-FI] sc init family={sc.family} type={sc.type} typeId={sc.typeId} typeName={sc.typeName}");

      // basePoint from location if available
      Dbg.W($"[SC-FI] location={fi?.Location?.GetType()?.FullName ?? "<null>"}");
      if (fi.Location is DB.LocationPoint lp && lp.Point is DB.XYZ p)
      {
        sc.basePoint = new Objects.Geometry.Point { x = p.X, y = p.Y, z = p.Z, units = ModelUnits };
        Dbg.W($"[SC-FI] basePoint set ({sc.basePoint.x},{sc.basePoint.y},{sc.basePoint.z}) units={sc.basePoint.units}");
      }
      Dbg.W($"[SC-FI] basePoint now {(sc.basePoint == null ? "<null>" : $"({sc.basePoint.x},{sc.basePoint.y},{sc.basePoint.z}) {sc.basePoint.units}")}");

      // NEW: read level from common built-ins with sensible fallbacks
      // Try FAMILY_LEVEL_PARAM (typical), then SCHEDULE_LEVEL_PARAM, then INSTANCE_REFERENCE_LEVEL_PARAM,
      // and finally fi.LevelId if all else fails.
      sc.level =
          ConvertAndCacheLevel(fi, BuiltInParameter.FAMILY_LEVEL_PARAM)
       ?? ConvertAndCacheLevel(fi, BuiltInParameter.SCHEDULE_LEVEL_PARAM)
       ?? ConvertAndCacheLevel(fi, BuiltInParameter.INSTANCE_REFERENCE_LEVEL_PARAM);

      if (sc.level == null && fi.LevelId != DB.ElementId.InvalidElementId)
      {
        if (Doc.GetElement(fi.LevelId) is DB.Level lvl)
          sc.level = LevelToSpeckle(lvl);
      }
      Dbg.W($"[SC-FI] level={(sc.level?.name ?? "<null>")}");

      // detached display — instance-style (same as RevitInstance / symbol flow)
      Dbg.W("[SC-FI] display start (instance-style)");
      var tf = fi.GetTotalTransform();
      Dbg.W("[SC-FI] got transform");
      var raw = GetElementDisplayValue(fi, isConvertedAsInstance: true, transform: tf);
      Dbg.W("[SC-FI] display raw acquired (instance-style)");
      sc.displayValue = CoerceDisplayToMeshList(raw);
      Dbg.W($"[SC-FI] display done count={(sc.displayValue?.Count ?? -1)}");
      for (int i = 0; i < (sc.displayValue?.Count ?? 0); i++)
      {
        var m = sc.displayValue[i];
        Dbg.W($"[SC-FI] mesh[{i}] v={(m?.vertices?.Count ?? -1)} f={(m?.faces?.Count ?? -1)} units={(m?.units ?? "<null>")}");
      }

      Dbg.W("[SC-FI] params start");
      GetAllRevitParamsAndIds(sc, fi);
      Dbg.W("[SC-FI] params done");

      Dbg.W($"[SC-FI] RETURN uid={fi?.UniqueId ?? "<null>"}");
      return sc;
    }
    private DB.Level FindNearestLevel(XYZ pt)
    {
      return new FilteredElementCollector(Doc)
        .OfClass(typeof(DB.Level))
        .Cast<DB.Level>()
        .OrderBy(l => Math.Abs(l.Elevation - pt.Z))
        .FirstOrDefault();
    }












    // In Objects.Converter.Revit.ConverterRevit

    // No changes needed in this main method. It is correct.
    private DB.Element StructuralConnectionFamilyInstanceToNative(Objects.BuiltElements.Revit.StructuralConnection sc)
    {
      Dbg.W($"[SC-FI→Native] ENTER family='{sc?.family}' type='{sc?.type}'");

      if (sc == null) throw new Exception("StructuralConnection is null.");
      if (string.IsNullOrWhiteSpace(sc.family) || string.IsNullOrWhiteSpace(sc.type))
        throw new Exception($"SC missing family/type. family='{sc?.family}' type='{sc?.type}'");
      if (sc.basePoint == null)
        throw new Exception("StructuralConnection.basePoint is required for placement.");

      WaitForHosts(sc);

      var logicalBasePoint = PointToNative(sc.basePoint);

      var symbol = FindFamilySymbolByFamilyAndType(sc.family, sc.type)
                   ?? throw new Exception($"Could not find FamilySymbol for family='{sc.family}' and type='{sc.type}'.");
      if (!symbol.IsActive) symbol.Activate();

      var fp = symbol.Family?.FamilyPlacementType ?? DB.FamilyPlacementType.Invalid;

      if (fp == DB.FamilyPlacementType.WorkPlaneBased)
      {
        var hostIds = ResolveConnectedHostIds(sc, out _);
        var hostEl = hostIds.Count > 0 ? Doc.GetElement(hostIds[0]) : null;

        if (hostEl == null)
        {
          throw new Exception("A host element is required for this face-based Structural Connection but was not found.");
        }
        Dbg.W($"[SC-FI→Native] Host element found: {hostEl.Name} (ID: {hostEl.Id})");

        Face hostFace = null;
        XYZ placementPoint = null;
        XYZ faceNormal = null;

        // ATTEMPT 1: Use the provided logical base point.
        Dbg.W($"[SC-FI→Native] Attempt 1: Finding face near logical base point {logicalBasePoint}...");
        hostFace = TryGetNearestFace(hostEl, logicalBasePoint, out placementPoint, out faceNormal);

        // ATTEMPT 2: If fail, use the host's Bounding Box center.
        if (hostFace == null)
        {
          Dbg.W("[SC-FI→Native] Attempt 1 FAILED. Trying host's bounding box center.");
          var bbox = hostEl.get_BoundingBox(null);
          if (bbox != null)
          {
            var bboxCenter = (bbox.Min + bbox.Max) / 2.0;
            hostFace = TryGetNearestFace(hostEl, bboxCenter, out placementPoint, out faceNormal);
          }
        }

        // ATTEMPT 3 (LAST RESORT): If fail, use the host's own location point.
        if (hostFace == null && hostEl.Location is LocationPoint hostLocation)
        {
          Dbg.W("[SC-FI→Native] Attempt 2 FAILED. Trying host's own location point.");
          hostFace = TryGetNearestFace(hostEl, hostLocation.Point, out placementPoint, out faceNormal);
        }

        if (hostFace == null)
        {
          throw new Exception($"Failed to find any suitable face on host '{hostEl.Name}' (ID: {hostEl.Id}) after multiple attempts. The element's geometry might be missing or invalid at Fine detail level.");
        }

        Dbg.W($"[SC-FI→Native] SUCCESS finding face. Final placement point: {placementPoint}.");

        try
        {
          var faceRef = hostFace.Reference;
          XYZ xDirection = Math.Abs(faceNormal.DotProduct(XYZ.BasisZ)) > 0.99
              ? XYZ.BasisX
              : XYZ.BasisZ.CrossProduct(faceNormal).Normalize();

          var fi = Doc.Create.NewFamilyInstance(faceRef, placementPoint, xDirection, symbol);

          if (fi == null)
            throw new Exception("Revit API failed to create the family instance on the host face.");

          Dbg.W($"[SC-FI→Native] Placed instance {fi.Id} on face of host {hostEl.Id}.");

          if (Math.Abs(sc.rotationDeg) > 1e-9 && fi.Location is LocationPoint lp)
          {
            var rotationAxis = Line.CreateUnbound(lp.Point, faceNormal);
            lp.Rotate(rotationAxis, sc.rotationDeg * Math.PI / 180.0);
          }

          return fi;
        }
        catch (Exception ex)
        {
          throw new Exception($"An error occurred while placing the connection on the host face: {ex.Message}", ex);
        }
      }

      var level = FindNearestLevel(logicalBasePoint) ?? throw new Exception("No Level found to place StructuralConnection.");
      var fi_unhosted = Doc.Create.NewFamilyInstance(logicalBasePoint, symbol, level, StructuralType.NonStructural);

      if (fi_unhosted == null)
        throw new Exception("Failed to create FamilyInstance for StructuralConnection.");

      return fi_unhosted;
    }

    // --------------------------------------------------------------------------------------------------
    // FINAL VERSION OF HELPER METHOD
    // This version now handles nested geometry inside GeometryInstances.
    // --------------------------------------------------------------------------------------------------
    private Face TryGetNearestFace(Element host, XYZ searchPoint, out XYZ projectedPoint, out XYZ faceNormal)
    {
      projectedPoint = null;
      faceNormal = null;

      var opts = new Options
      {
        ComputeReferences = true,
        DetailLevel = ViewDetailLevel.Fine
      };

      var geom = host?.get_Geometry(opts);
      if (geom == null) return null;

      Face bestFace = null;
      IntersectionResult bestProjResult = null;
      double bestDist = double.MaxValue;

      // This local function will do the actual face searching on a given solid
      void FindFaceInSolid(Solid solid)
      {
        foreach (Face f in solid.Faces)
        {
          var proj = f.Project(searchPoint);
          if (proj == null) continue;
          if (proj.Distance < bestDist)
          {
            bestDist = proj.Distance;
            bestFace = f;
            bestProjResult = proj;
          }
        }
      }

      foreach (var g in geom)
      {
        // Case 1: The geometry is a solid.
        if (g is Solid s && s.Volume > 1e-9)
        {
          FindFaceInSolid(s);
        }
        // Case 2: The geometry is a container for other geometry.
        else if (g is GeometryInstance instance)
        {
          var instanceGeom = instance.GetInstanceGeometry();
          foreach (var nestedG in instanceGeom)
          {
            if (nestedG is Solid nestedS && nestedS.Volume > 1e-9)
            {
              FindFaceInSolid(nestedS);
            }
          }
        }
      }

      if (bestFace != null && bestProjResult != null)
      {
        projectedPoint = bestProjResult.XYZPoint;
        faceNormal = bestFace.ComputeDerivatives(bestProjResult.UVPoint).BasisZ.Normalize();
      }

      return bestFace;
    }
















    private void DumpSCCandidates(string fam, string typ)
    {
      try
      {
        var all = new FilteredElementCollector(Doc)
          .OfClass(typeof(DB.FamilySymbol))
          .Cast<DB.FamilySymbol>()
          .Where(s => (BuiltInCategory)s.Category.Id.IntegerValue == BuiltInCategory.OST_StructConnections)
          .Select(s => $"{s.Family?.Name ?? s.FamilyName} :: {s.Name}")
          .OrderBy(s => s)
          .ToList();

        Dbg.W($"[SC-FI→Native] available SC symbols ({all.Count}): {string.Join(" | ", all)}");

        // near matches
        var near = all.Where(s => s.IndexOf(fam ?? "", StringComparison.OrdinalIgnoreCase) >= 0
                               || s.IndexOf(typ ?? "", StringComparison.OrdinalIgnoreCase) >= 0).ToList();
        if (near.Count > 0)
          Dbg.W($"[SC-FI→Native] near matches: {string.Join(" | ", near)}");
      }
      catch (Exception ex) { Dbg.W($"[SC-FI→Native] DumpSCCandidates ex: {ex.Message}"); }
    }




    private DB.FamilySymbol FindFamilySymbolByFamilyAndType(string family, string type)
    {
      string fam = family?.Trim();
      string typ = type?.Trim();
      Dbg.W($"[SC-FindSymbol] search fam='{fam}' typ='{typ}'");

      if (string.IsNullOrWhiteSpace(fam) || string.IsNullOrWhiteSpace(typ))
      {
        Dbg.W("[SC-FindSymbol] missing fam/type");
        return null;
      }

      var matches = new FilteredElementCollector(Doc)
        .OfClass(typeof(DB.FamilySymbol))
        .Cast<DB.FamilySymbol>()
        .Where(s =>
          (string.Equals(s.FamilyName, fam, StringComparison.OrdinalIgnoreCase) ||
           (s.Family != null && string.Equals(s.Family.Name, fam, StringComparison.OrdinalIgnoreCase))) &&
          string.Equals(s.Name, typ, StringComparison.OrdinalIgnoreCase))
        .ToList();

      Dbg.W($"[SC-FindSymbol] strict matches={matches.Count} :: {string.Join(", ", matches.Select(m => $"{m.FamilyName}:{m.Name}"))}");

      if (matches.Count == 1) return matches[0];

      if (matches.Count == 0 && string.Equals(fam, typ, StringComparison.OrdinalIgnoreCase))
      {
        matches = new FilteredElementCollector(Doc)
          .OfClass(typeof(DB.FamilySymbol))
          .Cast<DB.FamilySymbol>()
          .Where(s =>
            string.Equals(s.Name, typ, StringComparison.OrdinalIgnoreCase) &&
            (string.Equals(s.FamilyName, typ, StringComparison.OrdinalIgnoreCase) ||
             (s.Family != null && string.Equals(s.Family.Name, typ, StringComparison.OrdinalIgnoreCase))))
          .ToList();

        Dbg.W($"[SC-FindSymbol] fam==typ matches={matches.Count}");
        if (matches.Count == 1) return matches[0];
      }

      if (matches.Count > 1)
      {
        var scCatId = new DB.ElementId((int)DB.BuiltInCategory.OST_StructConnections);
        var preferred = matches.FirstOrDefault(s => s.Category != null && s.Category.Id == scCatId);
        Dbg.W($"[SC-FindSymbol] preferred by category → {(preferred != null ? preferred.FamilyName + ":" + preferred.Name : "<none>")}");
        if (preferred != null) return preferred;
      }

      return matches.FirstOrDefault();
    }







    private void LogHostReadiness(BER.StructuralConnection sc)
    {
      try
      {
        var wantUids = new List<string>();

        // detached connected elements
        if (sc?.connectedElements != null)
        {
          foreach (var b in sc.connectedElements)
          {
            var uid = b?.applicationId ?? b?["applicationId"] as string;
            if (!string.IsNullOrWhiteSpace(uid)) wantUids.Add(uid);
          }
        }

        // legacy list
        if (sc?.connectedElementUniqueIds != null)
          wantUids.AddRange(sc.connectedElementUniqueIds.Where(u => !string.IsNullOrWhiteSpace(u)));

        wantUids = wantUids.Distinct().ToList();

        if (wantUids.Count == 0)
        {
          Dbg.W("[SC/hosts] no hosts requested by this SC");
          return;
        }

        Dbg.W($"[SC/hosts] requested={wantUids.Count}  idmap.count={_byAppId.Count}");

        foreach (var uid in wantUids)
        {
          var hit = TryRecentlyConvertedByAppId(uid);
          var byUid = Doc.GetElement(uid);
          var status =
            (hit != null ? "HIT:idmap" : "MISS:idmap") + "|" +
            (byUid != null ? "HIT:doc" : "MISS:doc");
          Dbg.W($"[SC/hosts] uid={uid} → {status} (docId={(byUid?.Id.IntegerValue.ToString() ?? "-")})");
        }
      }
      catch (Exception ex) { Dbg.W($"[SC/hosts] ex: {ex.Message}"); }
    }


    // ----------------------------
    // Speckle ➜ Revit (create handler)
    // ----------------------------
    public DB.Element StructuralConnectionToNative(BER.StructuralConnection sc)
    {
      Dbg.W($"[SC→Native] ENTER family='{sc?.family}' type='{sc?.type}' appId='{sc?.applicationId ?? "<no-appId>"}'");

      LogHostReadiness(sc); // ← new

      // if you are on the family-instance path:
      Dbg.W("[SC→Native] path=FamilyInstance (handler path is bypassed in current build)");


      return StructuralConnectionFamilyInstanceToNative(sc);

      // 1) Resolve connected hosts (prefer detached objects, then legacy ids)
      var connected = new List<DB.ElementId>();

      // From detached connectedElements (applicationId is the Revit UniqueId in our ToSpeckle)
      if (sc.connectedElements != null)
      {
        foreach (var b in sc.connectedElements)
        {
          var uid = b?.applicationId ?? b?["applicationId"] as string;
          if (!string.IsNullOrWhiteSpace(uid) && Doc.GetElement(uid) is DB.Element e1)
          {
            connected.Add(e1.Id);
            continue;
          }

          // Try elementId string/int if present
          var elIdStr = (b?["elementId"] as string) ?? (b?["id"] as string);
          if (int.TryParse(elIdStr, out var eid) && Doc.GetElement(new DB.ElementId(eid)) is DB.Element e2)
            connected.Add(e2.Id);
        }
      }

      // Fallback: legacy UniqueIds
      if (connected.Count == 0 && sc.connectedElementUniqueIds != null)
      {
        foreach (var uid in sc.connectedElementUniqueIds)
          if (Doc.GetElement(uid) is DB.Element e) connected.Add(e.Id);
      }

      // Fallback: single legacy element id / app id
      if (connected.Count == 0 && !string.IsNullOrWhiteSpace(sc.connectedElementId))
      {
        var el = Doc.GetElement(sc.connectedElementId)
              ?? GetExistingElementByApplicationId(sc.connectedElementId);
        if (el != null) connected.Add(el.Id);
      }

      if (connected.Count == 0)
        throw new Exception("No valid connected host element found for StructuralConnection.");

      // 2) Resolve a handler type (prefer new 'type', then legacy TypeId/TypeName)
      DB.ElementId handlerTypeId = DB.ElementId.InvalidElementId;

      if (!string.IsNullOrWhiteSpace(sc.typeId) && int.TryParse(sc.typeId, out var tInt))
        handlerTypeId = new DB.ElementId(tInt);
      else if (!string.IsNullOrWhiteSpace(sc.type))
      {
        var byName = new DB.FilteredElementCollector(Doc)
          .OfClass(typeof(STR.StructuralConnectionHandlerType))
          .Cast<STR.StructuralConnectionHandlerType>()
          .FirstOrDefault(x => x.Name == sc.type);
        if (byName != null) handlerTypeId = byName.Id;
      }
      else if (!string.IsNullOrWhiteSpace(sc.typeName))
      {
        var byName = new DB.FilteredElementCollector(Doc)
          .OfClass(typeof(STR.StructuralConnectionHandlerType))
          .Cast<STR.StructuralConnectionHandlerType>()
          .FirstOrDefault(x => x.Name == sc.typeName);
        if (byName != null) handlerTypeId = byName.Id;
      }

      if (handlerTypeId == DB.ElementId.InvalidElementId)
      {
        var anyType = new DB.FilteredElementCollector(Doc)
          .OfClass(typeof(STR.StructuralConnectionHandlerType))
          .Cast<STR.StructuralConnectionHandlerType>()
          .FirstOrDefault()
          ?? throw new Exception("No StructuralConnectionHandlerType found in the document.");
        handlerTypeId = anyType.Id;
      }

      // 3) Create the handler
      var handler = STR.StructuralConnectionHandler.Create(Doc, connected, handlerTypeId);

      // Optional props (legacy keeps working)
      if (!string.IsNullOrWhiteSpace(sc.approvalTypeId) && int.TryParse(sc.approvalTypeId, out var aInt))
        handler.ApprovalTypeId = new DB.ElementId(aInt);

      if (!string.IsNullOrWhiteSpace(sc.codeCheckingStatus))
      {
        var prop = handler.GetType().GetProperty("CodeCheckingStatus");
        if (prop != null && prop.PropertyType.IsEnum)
        {
          try { prop.SetValue(handler, Enum.Parse(prop.PropertyType, sc.codeCheckingStatus, true)); }
          catch { /* ignore */ }
        }
      }

      return handler;
    }


    // Find a FamilySymbol by FAMILY NAME + TYPE NAME (case-insensitive).
    private DB.FamilySymbol ResolveSC_FamilySymbol(BER.StructuralConnection sc, out string reason)
    {
      reason = null;

      var fam = sc.family?.Trim();
      var typ = sc.type?.Trim();

      if (string.IsNullOrWhiteSpace(fam) || string.IsNullOrWhiteSpace(typ))
      {
        reason = $"missing family/type (family='{fam}', type='{typ}')";
        return null;
      }

      var matches = new FilteredElementCollector(Doc)
        .OfClass(typeof(DB.FamilySymbol))
        .Cast<DB.FamilySymbol>()
        .Where(s =>
          s.Family != null &&
          (string.Equals(s.Family.Name, fam, StringComparison.OrdinalIgnoreCase) ||
           string.Equals(s.FamilyName, fam, StringComparison.OrdinalIgnoreCase)) &&
          string.Equals(s.Name, typ, StringComparison.OrdinalIgnoreCase))
        .ToList();

      if (matches.Count == 1)
        return matches[0];

      if (matches.Count > 1)
      {
        // If multiple, prefer ones actually in the Structural Connections category.
        var scCatId = new DB.ElementId((int)DB.BuiltInCategory.OST_StructConnections);
        var preferred = matches.FirstOrDefault(s => s.Category != null && s.Category.Id == scCatId);
        if (preferred != null) return preferred;
      }

      reason = matches.Count == 0 ? "no matches" : $"ambiguous matches ({matches.Count})";
      return null;
    }

    private object StructuralConnectionToNativeWrap(BER.StructuralConnection sc)
    {
      var ao = new ApplicationObject(sc.applicationId ?? sc.id,
                                     sc.speckle_type ?? nameof(BER.StructuralConnection))
      { applicationId = sc.applicationId };

      try
      {
        var fi = StructuralConnectionFamilyInstanceToNative(sc);
        ao.Update(status: ApplicationObject.State.Created, createdId: fi.Id.IntegerValue.ToString());
        Dbg.W($"[SC→Native] AO Created {fi.Id.IntegerValue}");
      }
      catch (Exception ex)
      {
        ao.Update(status: ApplicationObject.State.Failed, logItem: ex.Message);
        Dbg.W($"[SC→Native] AO Failed: {ex.Message}");
      }
      return ao;
    }






    private List<string> GetRequestedHostAppIds(BER.StructuralConnection sc)
    {
      var want = new List<string>();

      if (sc?.connectedElements != null)
        foreach (var b in sc.connectedElements)
        {
          var uid = b?.applicationId ?? b?["applicationId"] as string;
          if (!string.IsNullOrWhiteSpace(uid)) want.Add(uid);
        }

      if (sc?.connectedElementUniqueIds != null)
        want.AddRange(sc.connectedElementUniqueIds.Where(u => !string.IsNullOrWhiteSpace(u)));

      // 🔧 include your single-string host id
      if (!string.IsNullOrWhiteSpace(sc?.connectedElementId))
        want.Add(sc.connectedElementId);

      return want.Distinct().ToList();
    }


















    private IList<ElementId> ResolveConnectedHostIds(BER.StructuralConnection sc, out List<string> missingAppIds)
    {
      var result = new List<ElementId>();
      missingAppIds = new List<string>();

      foreach (var appId in GetRequestedHostAppIds(sc))
      {
        var recent = TryRecentlyConvertedByAppId(appId); // ← uses your _byAppId
        if (recent != null) { result.Add(recent.Id); continue; }

        var byUid = Doc.GetElement(appId);               // works only if it's a real Revit UniqueId
        if (byUid != null) { result.Add(byUid.Id); continue; }

        if (int.TryParse(appId, out var intId))
        {
          var byInt = Doc.GetElement(new ElementId(intId));
          if (byInt != null) { result.Add(byInt.Id); continue; }
        }

        missingAppIds.Add(appId);
      }

      return result;
    }
    // Small bounded wait so hosts created earlier in the same receive have time to hit _byAppId.
    private void WaitForHosts(BER.StructuralConnection sc, int timeoutMs = 3000, int pollMs = 50)
    {
      var want = GetRequestedHostAppIds(sc);
      if (want.Count == 0) { Dbg.W("[SC/wait] no hosts requested"); return; }

      var sw = System.Diagnostics.Stopwatch.StartNew();
      while (true)
      {
        var resolved = ResolveConnectedHostIds(sc, out var missing);
        Dbg.W($"[SC/wait] t={sw.ElapsedMilliseconds}ms  resolved=[{string.Join(",", resolved.Select(id => id.IntegerValue))}]  missing=[{string.Join(",", missing)}]  idmap={_byAppId.Count}");
        if (missing.Count == 0) break;

        if (sw.ElapsedMilliseconds >= timeoutMs)
        {
          Dbg.W($"[SC/wait] TIMEOUT after {sw.ElapsedMilliseconds}ms");
          break;
        }
        System.Threading.Thread.Sleep(pollMs);
      }
    }
    // True if the family is face-based.
    // Helper method to find the nearest face on a host element.
    private Reference TryGetNearestFaceReference(Element host, XYZ at)
    {
      var opts = new Options { ComputeReferences = true, DetailLevel = ViewDetailLevel.Fine };
      var geom = host?.get_Geometry(opts);
      if (geom == null) return null;

      Reference best = null;
      double bestDist = double.MaxValue;

      foreach (var g in geom)
      {
        if (g is Solid s && s.Volume > 1e-9)
          foreach (Face f in s.Faces)
          {
            var proj = f.Project(at);
            if (proj == null) continue;
            if (proj.Distance < bestDist)
            {
              bestDist = proj.Distance;
              best = f.Reference;
            }
          }
      }
      return best;
    }
    // ===========================
    // Helpers
    // ===========================
    private DB.Options GetFine3DOptions()
    {
      var opts = new DB.Options
      {
        DetailLevel = DB.ViewDetailLevel.Fine,
        ComputeReferences = true,
        IncludeNonVisibleObjects = true
      };

      if (ViewSpecificOptions?.View is DB.View3D v3) { opts.View = v3; }
      else
      {
        var any3D = new DB.FilteredElementCollector(Doc)
          .OfClass(typeof(DB.View3D))
          .Cast<DB.View3D>()
          .FirstOrDefault(x => !x.IsTemplate);
        if (any3D != null) opts.View = any3D;
      }
      return opts;
    }

    private IReadOnlyList<Base> CoerceDisplayToReadOnlyBaseList(object dv)
    {
      if (dv == null) return Array.Empty<Base>();

      if (dv is IReadOnlyList<Base> ro) return ro;

      if (dv is IEnumerable<Base> eBase)
        return eBase is List<Base> l ? l : new List<Base>(eBase);

      if (dv is System.Collections.IEnumerable any)
      {
        var list = new List<Base>();
        foreach (var it in any)
          if (it is Base b) list.Add(b);
        return list;
      }

      if (dv is Base single) return new[] { single };

      return Array.Empty<Base>();
    }
  }
}
