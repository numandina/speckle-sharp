#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Autodesk.Revit.DB;
using Avalonia.Threading;
using DesktopUI2;
using DesktopUI2.Models;
using DesktopUI2.Models.Settings;
using DesktopUI2.ViewModels;
using RevitSharedResources.Interfaces;
using RevitSharedResources.Models;
using Serilog.Context;
using Speckle.Core.Api;
using Speckle.Core.Kits;
using Speckle.Core.Logging;
using Speckle.Core.Models;
using Speckle.Core.Transports;

namespace Speckle.ConnectorRevit.UI;

public partial class ConnectorBindingsRevit
{
  // used to store the Stream State settings when sending/receiving
  private List<ISetting>? CurrentSettings { get; set; }

  /// <summary>
  /// Converts the Revit elements that have been added to the stream by the user, sends them to
  /// the Server and the local DB, and creates a commit with the objects.
  /// </summary>
  /// <param name="state">StreamState passed by the UI</param>
  [SuppressMessage("Design", "CA1031:Do not catch general exception types")]
  [SuppressMessage("Design", "CA1031:Do not catch general exception types")]
  public override async Task<string> SendStream(StreamState state, ProgressViewModel progress)
  {
    Dbg.W($"[SendStream] ENTER streamId={state?.StreamId ?? "<null>"} branch={state?.BranchName ?? "<null>"}");

    using var ctx = RevitConverterState.Push();

    // make sure to instance a new copy so all values are reset correctly
    var converter = (ISpeckleConverter)Activator.CreateInstance(Converter.GetType());
    Dbg.W($"[SendStream] converter instance created type={converter?.GetType()?.FullName ?? "<null>"}");
    converter.SetContextDocument(CurrentDoc.Document);
    converter.Report.ReportObjects.Clear();

    // set converter settings as tuples (setting slug, setting selection)
    var settings = new Dictionary<string, string>();
    CurrentSettings = state.Settings;
    Dbg.W($"[SendStream] settings count={CurrentSettings?.Count ?? 0}");
    foreach (var setting in state.Settings)
      settings.Add(setting.Slug, setting.Selection);

    converter.SetConverterSettings(settings);
    Dbg.W("[SendStream] converter settings applied");

    var streamId = state.StreamId;
    var client = state.Client;

    // collect selected objects (must be in API context)
    var selectedObjects = await APIContext
      .Run(_ => GetSelectionFilterObjects(converter, state.Filter))
      .ConfigureAwait(false);

    Dbg.W($"[SendStream] selectedObjects pre-descendants={selectedObjects?.Count ?? -1}");
    selectedObjects = HandleSelectedObjectDescendants(selectedObjects).ToList();
    Dbg.W($"[SendStream] selectedObjects with descendants={selectedObjects.Count}");

    // peek a few
    int peek = 0;
    foreach (var e in selectedObjects)
    {
      if (peek++ >= 5) break;
      Dbg.W($"[SendStream] sample type={e?.GetType()?.FullName ?? "<null>"} uid={e?.UniqueId ?? "<null>"} cat={(BuiltInCategory?)e?.Category?.Id.IntegerValue}");
    }

    state.SelectedObjectIds = selectedObjects.Select(x => x.UniqueId).Distinct().ToList();
    Dbg.W($"[SendStream] distinct SelectedObjectIds={state.SelectedObjectIds?.Count ?? -1}");

    if (!selectedObjects.Any())
      throw new InvalidOperationException("There are zero objects to send. Please use a filter, or set some via selection.");

    converter.SetContextDocument(revitDocumentAggregateCache);
    converter.SetContextObjects(
      selectedObjects
        .Select(x => new ApplicationObject(x.UniqueId, x.GetType().ToString()) { applicationId = x.UniqueId })
        .ToList()
    );
    Dbg.W($"[SendStream] converter context set (objects={selectedObjects.Count})");

    var commitObject = converter.ConvertToSpeckle(CurrentDoc.Document) ?? new Collection();
    Dbg.W($"[SendStream] model root created type={commitObject?.GetType()?.FullName ?? "<null>"}");

    IRevitCommitObjectBuilder commitObjectBuilder;
    if (converter is not IRevitCommitObjectBuilderExposer builderExposer)
      throw new Exception($"Converter {converter.Name} by {converter.Author} does not provide the necessary object, {nameof(IRevitCommitObjectBuilder)}, needed to build the Speckle commit object.");
    else
      commitObjectBuilder = builderExposer.commitObjectBuilder;

    Dbg.W("[SendStream] got commitObjectBuilder");

    progress.Report = new ProgressReport();
    progress.Max = selectedObjects.Count;

    var conversionProgressDict = new ConcurrentDictionary<string, int> { ["Conversion"] = 0 };
    var convertedCount = 0;

    // track object types for mixpanel logging
    Dictionary<string, int> typeCountDict = new();

    await APIContext
      .Run(() =>
      {
        using var d0 = LogContext.PushProperty("converterName", converter.Name);
        using var d1 = LogContext.PushProperty("converterAuthor", converter.Author);
        using var d2 = LogContext.PushProperty("conversionDirection", nameof(ISpeckleConverter.ConvertToSpeckle));
        using var d3 = LogContext.PushProperty("converterSettings", settings);

        Dbg.W("[SendStream] conversion loop begin");

        foreach (var revitElement in selectedObjects)
        {
          if (progress.CancellationToken.IsCancellationRequested)
            break;

          // log selection object type
          var revitObjectType = revitElement.GetType().ToString();
          typeCountDict.TryGetValue(revitObjectType, out var currentCount);
          typeCountDict[revitObjectType] = ++currentCount;

          bool isAlreadyConverted = GetOrCreateApplicationObject(revitElement, converter.Report, out ApplicationObject reportObj);
          Dbg.W($"[loop] uid={revitElement.UniqueId} type={revitElement.GetType().FullName} cat={(BuiltInCategory?)revitElement.Category?.Id.IntegerValue} alreadyConverted={isAlreadyConverted}");
          if (isAlreadyConverted)
            continue;

          progress.Report.Log(reportObj);

          // Add context to logger
          using var _d3 = LogContext.PushProperty("fromType", revitElement.GetType());
          using var _d4 = LogContext.PushProperty("elementCategory", revitElement.Category?.Name);

          try
          {
            converter.Report.Log(reportObj); // Log object so converter can access
            Dbg.W($"[loop] ConvertToSpeckle call uid={revitElement.UniqueId}");

            Base result = ConvertToSpeckle(revitElement, converter);

            // dv peek
            int dvCount = -1;
            try
            {
              var dv = result?["displayValue"] as System.Collections.IEnumerable;
              dvCount = 0; if (dv != null) foreach (var _ in dv) dvCount++;
            }
            catch { }

            Dbg.W($"[loop] converted uid={revitElement.UniqueId} speckleType={result?.speckle_type ?? "<null>"} dvCount={dvCount}");

            // log converted object
            reportObj.Update(
              status: ApplicationObject.State.Created,
              logItem: $"Sent as {ConnectorRevitUtils.SimplifySpeckleType(result.speckle_type)}"
            );

            if (result.applicationId != reportObj.applicationId)
            {
              SpeckleLog.Logger.Information(
                "Conversion result of type {elementType} has a different application Id ({actualId}) to the report object {expectedId}",
                revitElement.GetType(),
                result.applicationId,
                reportObj.applicationId
              );
              result.applicationId = reportObj.applicationId;
            }

            commitObjectBuilder.IncludeObject(result, revitElement);
            Dbg.W($"[loop] included uid={revitElement.UniqueId}");
            convertedCount++;
          }
          catch (Exception ex)
          {
            Dbg.W($"[loop] EX uid={revitElement.UniqueId} ex={ex}");
            ConnectorHelpers.LogConversionException(ex);

            var failureStatus = ConnectorHelpers.GetAppObjectFailureState(ex);
            reportObj.Update(status: failureStatus, logItem: ex.Message);
          }

          conversionProgressDict["Conversion"]++;
          progress.Update(conversionProgressDict);

          YieldToUIThread(TimeSpan.FromMilliseconds(1));
        }

        Dbg.W($"[SendStream] conversion loop end convertedCount={convertedCount}");
      })
      .ConfigureAwait(false);

    revitDocumentAggregateCache.InvalidateAll();

    progress.Report.Merge(converter.Report);

    progress.CancellationToken.ThrowIfCancellationRequested();

    if (convertedCount == 0)
      throw new SpeckleException("Zero objects converted successfully. Send stopped.");

    // track the object type counts as an event before we try to send
    var typeCountList = typeCountDict
      .Select(o => new { TypeName = o.Key, Count = o.Value })
      .OrderBy(pair => pair.Count)
      .Reverse()
      .Take(200);

    Analytics.TrackEvent(
      Analytics.Events.ConvertToSpeckle,
      new Dictionary<string, object>() { { "typeCount", typeCountList } }
    );

    Dbg.W("[commit] BuildCommitObject begin");
    commitObjectBuilder.BuildCommitObject(commitObject);
    try
    {
      var elems = commitObject?["elements"] as System.Collections.IEnumerable;
      int eCount = 0; if (elems != null) foreach (var _ in elems) eCount++;
      Dbg.W($"[commit] root={commitObject?.GetType()?.FullName ?? "<null>"} elements count={eCount}");
    }
    catch { Dbg.W("[commit] root elements introspection failed"); }

    var transports = new List<ITransport>() { new ServerTransport(client.Account, streamId) };

    Dbg.W("[send] Operations.Send begin");
    var objectId = await Operations
      .Send(
        @object: commitObject,
        cancellationToken: progress.CancellationToken,
        transports: transports,
        onProgressAction: dict => progress.Update(dict),
        onErrorAction: ConnectorHelpers.DefaultSendErrorHandler,
        disposeTransports: true
      )
      .ConfigureAwait(true);
    Dbg.W($"[send] Operations.Send done objectId={objectId}");

    progress.CancellationToken.ThrowIfCancellationRequested();

    var actualCommit = new CommitCreateInput()
    {
      streamId = streamId,
      objectId = objectId,
      branchName = state.BranchName,
      message = state.CommitMessage ?? $"Sent {convertedCount} objects from {ConnectorRevitUtils.RevitAppName}.",
      sourceApplication = ConnectorRevitUtils.RevitAppName,
    };

    if (state.PreviousCommitId != null)
      actualCommit.parents = new List<string>() { state.PreviousCommitId };

    Dbg.W("[commit] CreateCommit begin");
    var commitId = await ConnectorHelpers
      .CreateCommit(client, actualCommit, progress.CancellationToken)
      .ConfigureAwait(false);
    Dbg.W($"[commit] CreateCommit done commitId={commitId}");

    Dbg.W("[SendStream] EXIT");
    return commitId;
  }








  public static bool GetOrCreateApplicationObject(
    Element revitElement,
    ProgressReport report,
    out ApplicationObject reportObj
  )
  {
    if (report.ReportObjects.TryGetValue(revitElement.UniqueId, out var applicationObject))
    {
      reportObj = applicationObject;
      return true;
    }

    string descriptor = ConnectorRevitUtils.ObjectDescriptor(revitElement);
    reportObj = new(revitElement.UniqueId, descriptor) { applicationId = revitElement.UniqueId };
    return false;
  }

  private DateTime timerStarted = DateTime.MinValue;

  private void YieldToUIThread(TimeSpan delay)
  {
    var currentTime = DateTime.UtcNow;

    if (currentTime.Subtract(timerStarted) < TimeSpan.FromSeconds(.15))
    {
      return;
    }

    using CancellationTokenSource s = new(delay);
    Dispatcher.UIThread.MainLoop(s.Token);
    timerStarted = currentTime;
  }

  private static Base ConvertToSpeckle(Element revitElement, ISpeckleConverter converter)
  {
    if (!converter.CanConvertToSpeckle(revitElement))
    {
      string skipMessage = revitElement switch
      {
        RevitLinkInstance => "Enable linked model support from the settings to send this object",
        _ => "Sending this object type is not supported yet"
      };

      throw new ConversionSkippedException(skipMessage, revitElement);
    }

    Base conversionResult = converter.ConvertToSpeckle(revitElement);

    if (conversionResult == null)
    {
      throw new SpeckleException(
        $"Conversion of {revitElement.GetType().Name} with id {revitElement.Id} (ToSpeckle) returned null"
      );
    }

    return conversionResult;
  }
}
