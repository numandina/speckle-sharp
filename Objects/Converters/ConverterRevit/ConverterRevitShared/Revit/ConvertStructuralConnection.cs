// Revit/ConvertStructuralConnection.cs
#nullable enable
using System;
using System.Linq;
using System.Collections.Generic;
using Speckle.Core.Models;
using Objects.BuiltElements.Revit;
using DB = Autodesk.Revit.DB;
using STR = Autodesk.Revit.DB.Structure;

namespace Objects.Converter.Revit
{
  public partial class ConverterRevit
  {
    // ================================
    // Minimal: Speckle -> Revit
    // - No rotation, no position, no offsets, no faces
    // - Just create/update a StructuralConnectionHandler from the connected elements
    // ================================
    private ApplicationObject StructuralConnectionToNative(StructuralConnection sc)
    {
      var appObj = new ApplicationObject(sc.id, sc.speckle_type) { applicationId = sc.applicationId };

      // 1) Resolve connected Revit element ids (by your existing indexer)
      var hostIds = new List<DB.ElementId>();
      foreach (var key in sc.connectedElementUniqueIds ?? Enumerable.Empty<string>())
      {
        var ids = FindByAppId(key);
        if (ids.Count > 0) hostIds.Add(ids[0]);
      }

      if (hostIds.Count == 0)
      {
        appObj.Update(status: ApplicationObject.State.Skipped, logItem: "No connected Revit elements resolved.");
        return appObj;
      }

      // 2) Find existing handler by Speckle.ApplicationId (if any)
      STR.StructuralConnectionHandler? existing = null;
      if (!string.IsNullOrWhiteSpace(sc.applicationId))
      {
        try
        {
          existing = new DB.FilteredElementCollector(Doc)
            .OfClass(typeof(STR.StructuralConnectionHandler))
            .Cast<STR.StructuralConnectionHandler>()
            .FirstOrDefault(e =>
            {
              var p = e.LookupParameter("Speckle.ApplicationId");
              return p != null && string.Equals(p.AsString(), sc.applicationId, StringComparison.OrdinalIgnoreCase);
            });
        }
        catch { /* ignore */ }
      }

      // 3) Helper to get a connection type (optional)
      DB.ElementType? pickType()
      {
        // by numeric id
        if (int.TryParse(sc.typeId, out var typeInt))
        {
          if (Doc.GetElement(new DB.ElementId(typeInt)) is DB.ElementType t) return t;
        }
        // by name
        if (!string.IsNullOrWhiteSpace(sc.typeName))
        {
          var byName = new DB.FilteredElementCollector(Doc)
            .OfClass(typeof(DB.ElementType)).Cast<DB.ElementType>()
            .FirstOrDefault(t =>
              t.Category != null &&
              (DB.BuiltInCategory)t.Category.Id.IntegerValue == DB.BuiltInCategory.OST_StructConnections &&
              t.Name.Equals(sc.typeName, StringComparison.OrdinalIgnoreCase));
          if (byName != null) return byName;
        }
        // first available
        return new DB.FilteredElementCollector(Doc)
          .OfClass(typeof(DB.ElementType)).Cast<DB.ElementType>()
          .FirstOrDefault(t =>
            t.Category != null &&
            (DB.BuiltInCategory)t.Category.Id.IntegerValue == DB.BuiltInCategory.OST_StructConnections);
      }

      void setSpeckleId(DB.Element el)
      {
        try
        {
          var p = el.LookupParameter("Speckle.ApplicationId");
          if (p != null && !p.IsReadOnly) p.Set(sc.applicationId ?? "");
        }
        catch { /* ignore */ }
      }

      void CreateOrUpdate()
      {
        var desiredType = pickType();

        if (existing == null)
        {
          // Just create the handler from the elements; Revit handles placement/orientation.
          var handler = STR.StructuralConnectionHandler.Create(Doc, hostIds);
          if (desiredType != null && handler.GetTypeId() != desiredType.Id)
            handler.ChangeTypeId(desiredType.Id);

          setSpeckleId(handler);
          appObj.Update(status: ApplicationObject.State.Created, createdId: handler.UniqueId);
          return;
        }

        // Update: only type + stamp id
        if (desiredType != null && existing.GetTypeId() != desiredType.Id)
          existing.ChangeTypeId(desiredType.Id);

        setSpeckleId(existing);
        appObj.Update(status: ApplicationObject.State.Updated, createdId: existing.UniqueId);
      }

      // 4) Transaction wrapper
      if (Doc.IsModifiable)
      {
        using var st = new DB.SubTransaction(Doc);
        st.Start(); CreateOrUpdate(); st.Commit();
      }
      else
      {
        using var tx = new DB.Transaction(Doc, (existing == null) ? "Speckle - Create Structural Connection" : "Speckle - Update Structural Connection");
        tx.Start(); CreateOrUpdate(); tx.Commit();
      }

      return appObj;
    }
  }
}
