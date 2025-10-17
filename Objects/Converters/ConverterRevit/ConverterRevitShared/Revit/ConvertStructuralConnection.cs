// Revit/ConvertStructuralConnection.cs
#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Autodesk.Revit.DB;
using Autodesk.Revit.DB.Structure;
using Objects.BuiltElements.Revit;
using Speckle.Core.Models;

namespace Objects.Converter.Revit
{
  public partial class ConverterRevit
  {






    private static readonly string SC_LOG_PATH = Path.Combine(
  Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
  "Speckle", "Logs", "SC_debug.log"
);

    // extras to make logging robust
    private static readonly object __SC_LOG_LOCK = new object();
    private static System.IO.StreamWriter __SC_LOG_WRITER;
    private static string __SC_LOG_ACTIVE_PATH = SC_LOG_PATH;
    private static bool __SC_LOG_INIT;

    private static void __SC_LOG_INIT_ONCE()
    {
      if (__SC_LOG_INIT) return;
      lock (__SC_LOG_LOCK)
      {
        if (__SC_LOG_INIT) return;

        bool TryOpen(string path)
        {
          try
          {
            var dir = System.IO.Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir)) System.IO.Directory.CreateDirectory(dir);
            var fs = new System.IO.FileStream(path, System.IO.FileMode.Append, System.IO.FileAccess.Write, System.IO.FileShare.ReadWrite);
            __SC_LOG_WRITER = new System.IO.StreamWriter(fs, new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: false))
            {
              AutoFlush = true
            };
            __SC_LOG_ACTIVE_PATH = path;
            __SC_LOG_WRITER.WriteLine($"----- SC LOG START {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} -----");
            __SC_LOG_WRITER.Flush();
            return true;
          }
          catch { return false; }
        }

        // try LocalAppData → AppData → Temp
        if (!TryOpen(SC_LOG_PATH))
        {
          var appData = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Speckle", "Logs", "SC_debug.log");
          if (!TryOpen(appData))
          {
            var temp = Path.Combine(Path.GetTempPath(), "Speckle", "Logs", "SC_debug.log");
            TryOpen(temp); // last resort (may still fail)
          }
        }

        __SC_LOG_INIT = true;
      }
    }

    private static void SC_LOG(string msg)
    {
      try
      {
        if (!__SC_LOG_INIT) __SC_LOG_INIT_ONCE();
        var line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] {msg}";

        lock (__SC_LOG_LOCK)
        {
          if (__SC_LOG_WRITER != null)
          {
            try { __SC_LOG_WRITER.WriteLine(line); }
            catch
            {
              // fallback if the stream suddenly dies
              System.Diagnostics.Debug.WriteLine(line);
              System.Diagnostics.Trace.WriteLine(line);
            }
          }
          else
          {
            // ultimate fallback
            System.Diagnostics.Debug.WriteLine(line);
            System.Diagnostics.Trace.WriteLine(line);
          }
        }
      }
      catch
      {
        // never throw from logging
      }
    }








    // -----------------------------
    // Revit -> Speckle
    // -----------------------------
    private Base StructuralConnectionToSpeckle(StructuralConnectionHandler sch)
    {
      var doc = sch.Document;

      var sc = new Objects.BuiltElements.Revit.StructuralConnection
      {
        applicationId = sch.UniqueId
      };

      // type id + name
      var tId = sch.GetTypeId();
      if (tId != null && tId != ElementId.InvalidElementId)
      {
        sc.typeId = tId.IntegerValue.ToString();
        sc.typeName = doc.GetElement(tId)?.Name;
      }

      // approval / code-checking
      try { sc.approvalTypeId = sch.ApprovalTypeId?.IntegerValue.ToString(); } catch { }
      try { sc.codeCheckingStatus = sch.CodeCheckingStatus.ToString(); } catch { }

      // connected elements (store UniqueIds)
      foreach (var id in sch.GetConnectedElementIds())
        if (doc.GetElement(id) is Element el)
          sc.connectedElementUniqueIds.Add(el.UniqueId);

      return sc;
    }

    // -----------------------------
    // Speckle -> Revit (2025-only)
    // ----------------------------




    // --- REPLACE YOUR METHOD WITH THIS ---
    private ApplicationObject StructuralConnectionToNative(Objects.BuiltElements.Revit.StructuralConnection sc)
    {
      var appObj = new ApplicationObject(sc.id, sc.speckle_type) { applicationId = sc.applicationId };

      // 1) Resolve hosts by applicationId
      var memberIds = new List<Autodesk.Revit.DB.ElementId>();
      foreach (var key in sc.connectedElementUniqueIds ?? Enumerable.Empty<string>())
      {
        var ids = FindByAppId(key);
        if (ids.Count > 0) memberIds.AddRange(ids);
      }

      if (memberIds.Count < 1)
      {
        if (!_isFlushingSC)
        {
          var pendKey = sc.applicationId ?? sc.id ?? Guid.NewGuid().ToString("N");
          if (_pendingSCKeys.Add(pendKey)) _pendingSC.Add(sc);
          appObj.Update(status: ApplicationObject.State.Skipped, logItem: "Deferred: waiting for host.");
        }
        else
        {
          appObj.Update(status: ApplicationObject.State.Skipped, logItem: "Missing host after flush.");
        }
        return appObj;
      }

      // 2) Pick a Structural Connections FamilySymbol
      Autodesk.Revit.DB.FamilySymbol symbol = null;

      if (int.TryParse(sc.typeId, out var typeInt))
      {
        var fsById = Doc.GetElement(new Autodesk.Revit.DB.ElementId(typeInt)) as Autodesk.Revit.DB.FamilySymbol;
        if (fsById?.Category != null &&
            (Autodesk.Revit.DB.BuiltInCategory)fsById.Category.Id.IntegerValue == Autodesk.Revit.DB.BuiltInCategory.OST_StructConnections)
          symbol = fsById;
      }

      if (symbol == null && !string.IsNullOrWhiteSpace(sc.typeName))
      {
        symbol = new Autodesk.Revit.DB.FilteredElementCollector(Doc)
          .OfClass(typeof(Autodesk.Revit.DB.FamilySymbol))
          .Cast<Autodesk.Revit.DB.FamilySymbol>()
          .FirstOrDefault(s =>
            s.Category != null
            && (Autodesk.Revit.DB.BuiltInCategory)s.Category.Id.IntegerValue == Autodesk.Revit.DB.BuiltInCategory.OST_StructConnections
            && s.Name.Equals(sc.typeName, StringComparison.OrdinalIgnoreCase));
      }

      if (symbol == null)
      {
        symbol = new Autodesk.Revit.DB.FilteredElementCollector(Doc)
          .OfClass(typeof(Autodesk.Revit.DB.FamilySymbol))
          .Cast<Autodesk.Revit.DB.FamilySymbol>()
          .FirstOrDefault(s =>
            s.Category != null
            && (Autodesk.Revit.DB.BuiltInCategory)s.Category.Id.IntegerValue == Autodesk.Revit.DB.BuiltInCategory.OST_StructConnections);
      }

      if (symbol == null)
      {
        appObj.Update(status: ApplicationObject.State.Skipped, logItem: "No Structural Connections family type found.");
        return appObj;
      }

      // 3) Compute placement point from first host (optionally midpoint of first two)
      var hostA = Doc.GetElement(memberIds[0]);

      Autodesk.Revit.DB.XYZ GetCenter(Autodesk.Revit.DB.Element e)
      {
        var bb = e.get_BoundingBox(null);
        if (bb != null) return (bb.Min + bb.Max) * 0.5;
        if (e.Location is Autodesk.Revit.DB.LocationPoint lp) return lp.Point;
        if (e.Location is Autodesk.Revit.DB.LocationCurve lc) return lc.Curve.Evaluate(0.5, true);
        return Autodesk.Revit.DB.XYZ.Zero;
      }

      var desired = GetCenter(hostA);
      if (memberIds.Count >= 2)
      {
        var mid = MidpointOfElements(memberIds[0], memberIds[1]);
        if (mid != null) desired = mid;
      }

      // Precompute faceRef/normal ONCE and reuse (prevents shadowing errors)
      Autodesk.Revit.DB.XYZ faceNormalOuter;
      var faceRefOuter = FindNearestPlanarFaceReference(hostA, desired, out faceNormalOuter);

      var pt = desired;
      if (faceRefOuter != null)
      {
        var pf = hostA.GetGeometryObjectFromReference(faceRefOuter) as Autodesk.Revit.DB.PlanarFace;
        var proj = pf?.Project(desired);
        if (proj != null) pt = proj.XYZPoint;
      }

      // Direction in plane for face-based placement
      var refDirOuter = (Math.Abs(faceNormalOuter.DotProduct(Autodesk.Revit.DB.XYZ.BasisZ)) > 0.9)
          ? Autodesk.Revit.DB.XYZ.BasisX
          : Autodesk.Revit.DB.XYZ.BasisZ;

      // 4) Closest level to point
      var level = ClosestLevelToPoint(pt);

      // 5) Existing instance by Speckle.ApplicationId
      Autodesk.Revit.DB.FamilyInstance existing = null;
      if (!string.IsNullOrWhiteSpace(sc.applicationId))
      {
        existing = new Autodesk.Revit.DB.FilteredElementCollector(Doc)
          .OfClass(typeof(Autodesk.Revit.DB.FamilyInstance))
          .Cast<Autodesk.Revit.DB.FamilyInstance>()
          .FirstOrDefault(fi =>
          {
            var p = fi.LookupParameter("Speckle.ApplicationId");
            return p != null && string.Equals(p.AsString(), sc.applicationId, StringComparison.OrdinalIgnoreCase);
          });
      }

      // Ensure symbol active (works either inside or outside a tx)
      if (!symbol.IsActive)
      {
        if (Doc.IsModifiable) symbol.Activate();
        else { using var txAct = new Autodesk.Revit.DB.Transaction(Doc, "Activate SC family symbol"); txAct.Start(); symbol.Activate(); txAct.Commit(); }
      }

      void AfterCreateOrUpdate(Autodesk.Revit.DB.FamilyInstance fiLocal)
      {
        try
        {
          var p = fiLocal.LookupParameter("Speckle.ApplicationId");
          if (p != null && !p.IsReadOnly) p.Set(sc.applicationId ?? "");
        }
        catch { }

        TryStampHostIds(fiLocal, sc.connectedElementUniqueIds);
        ApplyRotationAndOffset(fiLocal, sc); // rotation is forced to 0 inside for this test
      }

      bool useSub = Doc.IsModifiable;
      string txName = (existing == null) ? "Speckle - Place SC Family" : "Speckle - Update SC Family";
      var primaryHostEl = hostA;

      if (useSub)
      {
        using var st = new Autodesk.Revit.DB.SubTransaction(Doc);
        st.Start();

        var fi = existing;

        if (fi == null)
        {
          // element-hosted
          try
          {
            fi = Doc.Create.NewFamilyInstance(pt, symbol, primaryHostEl, Autodesk.Revit.DB.Structure.StructuralType.NonStructural);
          }
          catch { /* try face-based next */ }

          // face-based (reuse precomputed faceRefOuter/refDirOuter)
          if (fi == null && faceRefOuter != null)
          {
            fi = Doc.Create.NewFamilyInstance(faceRefOuter, pt, refDirOuter, symbol);
          }

          // free/level fallback
          if (fi == null)
          {
            fi = (level != null)
              ? Doc.Create.NewFamilyInstance(pt, symbol, level, Autodesk.Revit.DB.Structure.StructuralType.NonStructural)
              : Doc.Create.NewFamilyInstance(pt, symbol, Autodesk.Revit.DB.Structure.StructuralType.NonStructural);
          }

          AfterCreateOrUpdate(fi);
          st.Commit();
          appObj.Update(status: ApplicationObject.State.Created, createdId: fi.UniqueId);
        }
        else
        {
          if (fi.Symbol?.Id != symbol.Id) fi.ChangeTypeId(symbol.Id);

          if (fi.Location is Autodesk.Revit.DB.LocationPoint lp)
          {
            var delta = pt - lp.Point;
            if (delta.GetLength() > 1e-6)
              Autodesk.Revit.DB.ElementTransformUtils.MoveElement(Doc, fi.Id, delta);
          }

          AfterCreateOrUpdate(fi);
          st.Commit();
          appObj.Update(status: ApplicationObject.State.Updated, createdId: fi.UniqueId);
        }

        return appObj;
      }
      else
      {
        using var tx = new Autodesk.Revit.DB.Transaction(Doc, txName);
        tx.Start();

        var fi = existing;

        if (fi == null)
        {
          // element-hosted
          try
          {
            fi = Doc.Create.NewFamilyInstance(pt, symbol, primaryHostEl, Autodesk.Revit.DB.Structure.StructuralType.NonStructural);
          }
          catch { /* try face-based next */ }

          // face-based (reuse precomputed faceRefOuter/refDirOuter)
          if (fi == null && faceRefOuter != null)
          {
            fi = Doc.Create.NewFamilyInstance(faceRefOuter, pt, refDirOuter, symbol);
          }

          // free/level fallback
          if (fi == null)
          {
            fi = (level != null)
              ? Doc.Create.NewFamilyInstance(pt, symbol, level, Autodesk.Revit.DB.Structure.StructuralType.NonStructural)
              : Doc.Create.NewFamilyInstance(pt, symbol, Autodesk.Revit.DB.Structure.StructuralType.NonStructural);
          }

          AfterCreateOrUpdate(fi);
          tx.Commit();
          appObj.Update(status: ApplicationObject.State.Created, createdId: fi.UniqueId);
        }
        else
        {
          if (fi.Symbol?.Id != symbol.Id) fi.ChangeTypeId(symbol.Id);

          if (fi.Location is Autodesk.Revit.DB.LocationPoint lp)
          {
            var delta = pt - lp.Point;
            if (delta.GetLength() > 1e-6)
              Autodesk.Revit.DB.ElementTransformUtils.MoveElement(Doc, fi.Id, delta);
          }

          AfterCreateOrUpdate(fi);
          tx.Commit();
          appObj.Update(status: ApplicationObject.State.Updated, createdId: fi.UniqueId);
        }

        return appObj;
      }
    }









    private Reference FindNearestPlanarFaceReference(Element host, XYZ near, out XYZ normal)
    {
      normal = Autodesk.Revit.DB.XYZ.BasisZ;
      try
      {
        var opts = new Options { ComputeReferences = true, DetailLevel = ViewDetailLevel.Fine };
        var geo = host.get_Geometry(opts);
        Reference bestRef = null; double best = double.MaxValue;

        foreach (var g in geo)
        {
          if (g is Solid s && s.Faces.Size > 0)
          {
            foreach (Face f in s.Faces)
            {
              if (f is PlanarFace pf)
              {
                var proj = pf.Project(near);
                if (proj != null && proj.Distance < best)
                {
                  best = proj.Distance;
                  bestRef = pf.Reference;
                  normal = pf.FaceNormal; // <- capture the normal
                }
              }
            }
          }
        }
        return bestRef;
      }
      catch { return null; }
    }























    // robust read of number-like dynamic props
    private static bool TryGetNumber(Speckle.Core.Models.Base b, string name, out double val)
    {
      val = 0;
      object o;
      try { o = b[name]; } catch { return false; }
      if (o == null) return false;

      switch (o)
      {
        case double d: val = d; return true;
        case float f: val = f; return true;
        case int i: val = i; return true;
        case long l: val = l; return true;
        case string s:
          return double.TryParse(s, System.Globalization.NumberStyles.Float,
                  System.Globalization.CultureInfo.InvariantCulture, out val);
        default: return false;
      }
    }



    // Keep your existing helpers: ProjectToPlane, AngleSignedAboutAxis, NormalizeAngle, TryGetNumber.
    // Replace ONLY this method.
    private void ApplyRotationAndOffset(Autodesk.Revit.DB.FamilyInstance fi, Speckle.Core.Models.Base sc)
    {
      return;
      // --- freeze current position ---
      var lp = fi.Location as Autodesk.Revit.DB.LocationPoint;
      var pre = lp?.Point ?? fi.GetTransform().Origin;

      // --- target angle (rad preferred, then deg) ---
      double target = 0.0;
      bool hasTarget =
          TryGetNumber(sc, "rotationRad", out target)
       || (TryGetNumber(sc, "rotationDeg", out var deg) && ((target = deg * Math.PI / 180.0) == target));

      // --- axis and in-plane reference (host face if available) ---
      Autodesk.Revit.DB.XYZ axisDir = Autodesk.Revit.DB.XYZ.BasisZ;
      Autodesk.Revit.DB.XYZ refX = Autodesk.Revit.DB.XYZ.BasisX;
      try
      {
        var hostRef = fi.HostFace;
        if (hostRef != null)
        {
          var hostEl = Doc.GetElement(hostRef.ElementId);
          if (hostEl?.GetGeometryObjectFromReference(hostRef) is Autodesk.Revit.DB.PlanarFace pf)
          {
            axisDir = pf.FaceNormal;
            refX = pf.XVector;
          }
        }
      }
      catch { /* fallback stays Z/X */ }

      // --- rotate by delta only (about axis through insertion point) ---
      if (hasTarget)
      {
        var cur = AngleSignedAboutAxis(ProjectToPlane(refX, axisDir), ProjectToPlane(fi.HandOrientation, axisDir), axisDir);
        var delta = NormalizeAngle(target - cur);
        if (Math.Abs(delta) > 1e-9)
        {
          var axis = Autodesk.Revit.DB.Line.CreateBound(pre, pre + axisDir);
          Autodesk.Revit.DB.ElementTransformUtils.RotateElement(Doc, fi.Id, axis, delta);
        }
      }

      // --- restore any drift introduced by rotation ---
      var post = (fi.Location as Autodesk.Revit.DB.LocationPoint)?.Point ?? fi.GetTransform().Origin;
      var drift = pre - post;
      if (drift.GetLength() > 1e-9)
        Autodesk.Revit.DB.ElementTransformUtils.MoveElement(Doc, fi.Id, drift);

      // --- apply offset last (same as before) ---
      if (TryGetNumber(sc, "offsetFromHost", out var offFeet) || TryGetNumber(sc, "hostOffset", out offFeet))
      {
        var p =
            fi.LookupParameter("Offset from Host")
         ?? fi.get_Parameter(Autodesk.Revit.DB.BuiltInParameter.INSTANCE_FREE_HOST_OFFSET_PARAM)
         ?? fi.get_Parameter(Autodesk.Revit.DB.BuiltInParameter.INSTANCE_ELEVATION_PARAM);

        if (p != null && !p.IsReadOnly) p.Set(offFeet);
        else if (Math.Abs(offFeet) > 1e-9)
          Autodesk.Revit.DB.ElementTransformUtils.MoveElement(Doc, fi.Id, new Autodesk.Revit.DB.XYZ(0, 0, offFeet));
      }
    }




    // ---- small helpers (same class) ----
    private static Autodesk.Revit.DB.XYZ ProjectToPlane(Autodesk.Revit.DB.XYZ v, Autodesk.Revit.DB.XYZ n)
    {
      var nn = n.Normalize();
      return v - (v.DotProduct(nn)) * nn;
    }

    private static double AngleSignedAboutAxis(Autodesk.Revit.DB.XYZ a, Autodesk.Revit.DB.XYZ b, Autodesk.Revit.DB.XYZ axis)
    {
      var an = a.Normalize();
      var bn = b.Normalize();
      double cos = Math.Max(-1.0, Math.Min(1.0, an.DotProduct(bn)));
      double ang = Math.Acos(cos);
      double sign = axis.Normalize().DotProduct(an.CrossProduct(bn));
      return sign < 0 ? -ang : ang;
    }

    private static double NormalizeAngle(double a)
    {
      const double TAU = Math.PI * 2.0;
      while (a > Math.PI) a -= TAU;
      while (a < -Math.PI) a += TAU;
      return a;
    }





























    // --- helpers used above (drop these anywhere in the same partial class) ---
    private static bool TryGetNumberParam(Autodesk.Revit.DB.Element el, string name, out double val)
    {
      val = 0;
      try
      {
        var p = el.LookupParameter(name);
        if (p == null) return false;

        switch (p.StorageType)
        {
          case Autodesk.Revit.DB.StorageType.Double: val = p.AsDouble(); return true;
          case Autodesk.Revit.DB.StorageType.Integer: val = p.AsInteger(); return true;
          case Autodesk.Revit.DB.StorageType.String:
            return double.TryParse(p.AsString(), System.Globalization.NumberStyles.Float,
                      System.Globalization.CultureInfo.InvariantCulture, out val);
          default: return false;
        }
      }
      catch { return false; }
    }

    private static void SetNumberOrTextParam(Autodesk.Revit.DB.Element el, string name, double val)
    {
      try
      {
        var p = el.LookupParameter(name);
        if (p == null || p.IsReadOnly) return;

        if (p.StorageType == Autodesk.Revit.DB.StorageType.Double) p.Set(val);
        else if (p.StorageType == Autodesk.Revit.DB.StorageType.Integer) p.Set((int)Math.Round(val));
        else p.Set(val.ToString(System.Globalization.CultureInfo.InvariantCulture));
      }
      catch { /* ignore */ }
    }















    private Autodesk.Revit.DB.Element FindExistingSCByAppId(string appId)
    {
      if (string.IsNullOrWhiteSpace(appId)) return null;
      try
      {
        return new Autodesk.Revit.DB.FilteredElementCollector(Doc)
          .WhereElementIsNotElementType()
          .Cast<Autodesk.Revit.DB.Element>()
          .FirstOrDefault(e =>
          {
            var p = e.LookupParameter("Speckle.ApplicationId");
            return p != null && string.Equals(p.AsString(), appId, StringComparison.OrdinalIgnoreCase);
          });
      }
      catch { return null; }
    }

    private ApplicationObject CreateOrUpdateHandler(
      Autodesk.Revit.DB.Structure.StructuralConnectionHandler existing,
      Autodesk.Revit.DB.ElementId typeId,
      IList<Autodesk.Revit.DB.ElementId> memberIds,
      Objects.BuiltElements.Revit.StructuralConnection sc,
      ApplicationObject appObj)
    {
      var h = existing;
      if (h == null)
      {
        SC_LOG($"[SC][CREATE][HANDLER] members={memberIds.Count} typeId={typeId.IntegerValue}");
        h = Autodesk.Revit.DB.Structure.StructuralConnectionHandler.Create(Doc, memberIds, typeId);
        SetSpeckleAppIdParam(h, sc.applicationId);
        TrySetProps(h, sc);
        appObj.Update(status: ApplicationObject.State.Created, createdId: h.UniqueId);
        return appObj;
      }

      var current = h.GetConnectedElementIds() ?? new List<Autodesk.Revit.DB.ElementId>();
      bool sameMembers = current.Count == memberIds.Count &&
                         !current.Except(memberIds).Any() &&
                         !memberIds.Except(current).Any();

      if (!sameMembers)
      {
        SC_LOG($"[SC][REPLACE][HANDLER] members changed; recreating. oldId={h.Id.IntegerValue}");
        var oldId = h.Id;
        Doc.Delete(oldId);
        h = Autodesk.Revit.DB.Structure.StructuralConnectionHandler.Create(Doc, memberIds, typeId);
        SetSpeckleAppIdParam(h, sc.applicationId);
      }
      else if (h.GetTypeId() != typeId)
      {
        SC_LOG($"[SC][TYPE][HANDLER] {h.GetTypeId().IntegerValue} -> {typeId.IntegerValue}");
        h.ChangeTypeId(typeId);
      }

      TrySetProps(h, sc);
      appObj.Update(status: sameMembers ? ApplicationObject.State.Updated : ApplicationObject.State.Created,
                    createdId: h.UniqueId);
      return appObj;
    }









    private ApplicationObject CreateOrUpdateFamily(
      Autodesk.Revit.DB.FamilyInstance existing,
      Autodesk.Revit.DB.FamilySymbol symbol,
      Autodesk.Revit.DB.Level level,
      Autodesk.Revit.DB.XYZ point,
      Objects.BuiltElements.Revit.StructuralConnection sc,
      ApplicationObject appObj)
    {
      if (!symbol.IsActive) symbol.Activate();

      if (existing == null)
      {
        SC_LOG($"[SC][CREATE][FAM] sym={symbol.Name} lvl={level.Name} at {point}");
        var fi = Doc.Create.NewFamilyInstance(
          point, symbol, level, Autodesk.Revit.DB.Structure.StructuralType.NonStructural);
        SetSpeckleAppIdParam(fi, sc.applicationId);
        ApplyRotationAndOffset(fi, sc);

        TryStampHostIds(fi, sc.connectedElementUniqueIds);
        appObj.Update(status: ApplicationObject.State.Created, createdId: fi.UniqueId);
        return appObj;
      }

      if (existing.Symbol?.Id != symbol.Id)
      {
        SC_LOG($"[SC][TYPE][FAM] {existing.Symbol?.Name} -> {symbol.Name}");
        existing.ChangeTypeId(symbol.Id);
      }

      if (existing.Location is Autodesk.Revit.DB.LocationPoint lp)
      {
        var delta = point - lp.Point;
        if (delta.GetLength() > 1e-6)
          Autodesk.Revit.DB.ElementTransformUtils.MoveElement(Doc, existing.Id, delta);
      }

      TryStampHostIds(existing, sc.connectedElementUniqueIds);

      appObj.Update(status: ApplicationObject.State.Updated, createdId: existing.UniqueId);
      return appObj;
    }

    private void TryStampHostIds(Autodesk.Revit.DB.Element el, IList<string> hostIds)
    {
      if (hostIds == null) return;
      var joined = string.Join(";", hostIds);
      foreach (var name in new[] { "Speckle.HostIds", "HostIds", "HostAId", "HostBId" })
      {
        var p = el.LookupParameter(name);
        if (p != null && !p.IsReadOnly) { p.Set(joined); break; }
      }
    }

    private Autodesk.Revit.DB.XYZ MidpointOfElements(Autodesk.Revit.DB.ElementId aId, Autodesk.Revit.DB.ElementId bId)
    {
      Autodesk.Revit.DB.XYZ GetCenter(Autodesk.Revit.DB.Element e)
      {
        var bb = e.get_BoundingBox(null);
        if (bb != null) return (bb.Min + bb.Max) * 0.5;
        if (e.Location is Autodesk.Revit.DB.LocationPoint lp) return lp.Point;
        if (e.Location is Autodesk.Revit.DB.LocationCurve lc) return lc.Curve.Evaluate(0.5, true);
        return Autodesk.Revit.DB.XYZ.Zero;
      }
      var a = Doc.GetElement(aId); var b = Doc.GetElement(bId);
      if (a == null || b == null) return null;
      return (GetCenter(a) + GetCenter(b)) * 0.5;
    }

    private Autodesk.Revit.DB.Level ClosestLevelToPoint(Autodesk.Revit.DB.XYZ p)
    {
      try
      {
        return new Autodesk.Revit.DB.FilteredElementCollector(Doc)
          .OfClass(typeof(Autodesk.Revit.DB.Level))
          .Cast<Autodesk.Revit.DB.Level>()
          .OrderBy(l => Math.Abs(l.Elevation - p.Z))
          .FirstOrDefault();
      }
      catch { return null; }
    }


    // OPTIONAL: stamp param for round-trip (only if you’ve added it to your template)
    private void SetSpeckleAppIdParam(Element el, string appId)
    {
      var p = el.LookupParameter("Speckle.ApplicationId");
      if (p != null && !p.IsReadOnly) p.Set(appId);
    }

    // -----------------------------
    // Speckle -> Revit (2025-only)
    // ----------------------------
    //
    // -
    private void TrySetProps(StructuralConnectionHandler handler, Objects.BuiltElements.Revit.StructuralConnection sc)
    {
      try
      {
        if (int.TryParse(sc.approvalTypeId, out var appr))
          handler.ApprovalTypeId = new ElementId(appr);
      }
      catch { /* ignore if not supported */ }

      try
      {
        if (!string.IsNullOrWhiteSpace(sc.codeCheckingStatus) &&
            Enum.TryParse<StructuralConnectionCodeCheckingStatus>(sc.codeCheckingStatus, true, out var status))
        {
          handler.CodeCheckingStatus = status;
        }
      }
      catch { /* ignore */ }
    }
  }
}
