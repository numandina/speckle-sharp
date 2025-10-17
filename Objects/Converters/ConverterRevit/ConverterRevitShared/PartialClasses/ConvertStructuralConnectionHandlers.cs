using Autodesk.Revit.DB;
using Speckle.Core.Models;
using DB = Autodesk.Revit.DB;
using Structure = Autodesk.Revit.DB.Structure;

namespace Objects.Converter.Revit;

public partial class ConverterRevit
{

  private Base StructuralConnectionHandlerToSpeckle(Autodesk.Revit.DB.Structure.StructuralConnectionHandler revitConnection)
  {
    var doc = revitConnection.Document;

    var sc = new Objects.BuiltElements.Revit.StructuralConnection
    {
      applicationId = revitConnection.UniqueId
    };
    sc["__probe"] = "SC converter hit";

    // type id + name
    var tId = revitConnection.GetTypeId();
    if (tId != null && tId != ElementId.InvalidElementId)
    {
      sc.typeId = tId.IntegerValue.ToString();
      var t = doc.GetElement(tId) as Autodesk.Revit.DB.Structure.StructuralConnectionHandlerType;
      sc.typeName = t?.Name;
    }

    // approval / code-checking (best-effort; API varies across years)
    try { sc.approvalTypeId = revitConnection.ApprovalTypeId?.IntegerValue.ToString(); } catch { }
    try { sc.codeCheckingStatus = revitConnection.CodeCheckingStatus.ToString(); } catch { }

    // connected elements (store UniqueIds)
    foreach (var id in revitConnection.GetConnectedElementIds())
    {
      var el = doc.GetElement(id);
      if (el != null) sc.connectedElementUniqueIds.Add(el.UniqueId);
    }

    // optional mesh for viewers
    sc.displayValue = GetElementDisplayValue(revitConnection);

    // params & ids onto the Speckle Base
    GetAllRevitParamsAndIds(sc, revitConnection);

    return sc;
  }
}
