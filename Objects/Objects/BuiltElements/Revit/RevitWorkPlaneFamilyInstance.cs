// Objects/BuiltElements/Revit/RevitWorkPlaneFamilyInstance.cs
using System.Collections.Generic;
using Speckle.Core.Models;
using Objects.Other.Revit;               // base RevitInstance
using OG = Objects.Geometry;

namespace Objects.BuiltElements.Revit
{
  public class RevitWorkPlaneFamilyInstance : RevitInstance, IDisplayValue<List<OG.Mesh>>
  {
    // REQUIRED for receive to Revit
    public string family { get; set; }                    // e.g. "CF_RCA"
    public string type { get; set; }                    // e.g. "C22597-R55"
    public RevitSymbolElementType symbol { get; set; }    // optional but ideal

    // nice-to-have
    public string category { get; set; }                  // "Structural Connections"
    public int? builtInCategory { get; set; }           // (int) BuiltInCategory

    // your custom fields
    public bool isWorkPlaneHosted { get; set; }
    public string sketchPlaneUniqueId { get; set; }
    public OG.Plane workPlane { get; set; }
    public string levelUniqueId { get; set; }
    public double hostOffset { get; set; }
    public double rotation { get; set; }
    public bool mirrored { get; set; }
    public OG.Vector facingOrientation { get; set; }
    public OG.Vector handOrientation { get; set; }
    public OG.Point placementPoint { get; set; }
    public new List<OG.Mesh> displayValue { get; set; }
  }
}
