// Objects.BuiltElements.Revit.StructuralConnection.cs
using System;
using System.Collections.Generic;
using Objects.Geometry;
using Speckle.Core.Models;
using BE = Objects.BuiltElements;

namespace Objects.BuiltElements.Revit
{
  // Keep the name "StructuralConnection" to avoid confusion with Revit's DB.Structure.StructuralConnectionHandler
  public class StructuralConnection : Base, IDisplayValue<List<Mesh>>
  {
    // NEW: match the pattern you showed
    public string family { get; set; }              // e.g., ElementType.FamilyName or FamilySymbol.FamilyName
    public string type { get; set; }                // e.g., Type name
    public Point basePoint { get; set; }            // origin/anchor, when available

    [DetachProperty]
    public List<Base> connectedElements { get; set; } = new(); // detached hosts
    public Objects.Geometry.Vector hostOffset { get; set; } // units respected

    [DetachProperty]
    public List<Mesh> displayValue { get; set; } = new();       // detached meshes
    public BE.Level level { get; set; }

    // ─────────────────────────────────────────────────────────
    // LEGACY (kept for compatibility; safe to delete later)
    public string typeId { get; set; }
    public string typeName { get; set; }
    public string approvalTypeId { get; set; }
    public string codeCheckingStatus { get; set; }

    public List<string> connectedElementUniqueIds { get; set; } = new(); // legacy host references
    public string connectedElementId { get; set; }                        // legacy single host

    public double rotationDeg { get; set; }
    public double offsetFromHostFeet { get; set; }

    public string builtInCategory { get; set; } = "OST_StructConnections";

    // Map old "position" to new "basePoint"
    [Obsolete("Use basePoint instead")]
    public Point position
    {
      get => basePoint;
      set => basePoint = value;
    }
  }
}
