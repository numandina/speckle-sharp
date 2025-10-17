using System.Collections.Generic;
using Speckle.Core.Models;     // Base
using Objects.Geometry;        // Mesh

namespace Objects.BuiltElements.Revit
{
  public class StructuralConnection : Base
  {
    public string typeId { get; set; }
    public string typeName { get; set; }
    public string approvalTypeId { get; set; }
    public string codeCheckingStatus { get; set; }

    public List<string> connectedElementUniqueIds { get; set; } = new();

    // Optional: simple placeholder geometry for viewers/Unity
    public List<Mesh> displayValue { get; set; }

    public override string speckle_type => "Objects.BuiltElements.Revit.StructuralConnection";
  }
}
