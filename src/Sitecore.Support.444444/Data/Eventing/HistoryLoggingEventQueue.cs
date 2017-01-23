namespace Sitecore.Support.Data.Eventing
{
  using System;
  using System.Collections.Generic;
  using System.Diagnostics;
  using System.Linq;
  using Sitecore.Configuration;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Data.Eventing;
  using Sitecore.Data.Eventing.Remote;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;
  using Sitecore.Eventing.Remote;
  using Sitecore.SecurityModel;
  using Sitecore.StringExtensions;
  using Sitecore.Support.Data.History;
  using Sitecore.Support.Diagnostics;

  [UsedImplicitly]
  public class HistoryLoggingEventQueue : NoLockSqlServerEventQueue
  {
    private const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";

    [NotNull]
    protected readonly string DatabaseName;

    protected readonly bool HistoryEnabled;

    protected readonly bool HistoryDetailsEnabled;

    private readonly TimeSpan LogInterval;

    [NotNull]
    private readonly Stopwatch TotalTime = new Stopwatch();

    [NotNull]
    private readonly Stopwatch ReadTime = new Stopwatch();

    [NotNull]
    private readonly Stopwatch DeserializeTime = new Stopwatch();

    [NotNull]
    private readonly Stopwatch ProcessTime = new Stopwatch();

    [NotNull]
    private readonly SimpleCounter EventsCount = new SimpleCounter();

    public HistoryLoggingEventQueue([NotNull] SqlDataApi api, [NotNull] Database database)
      : base(api, database)
    {
      Assert.ArgumentNotNull(api, nameof(api));
      Assert.ArgumentNotNull(database, nameof(database));

      var databaseName = database.Name;

      HistoryEnabled = HistorySettings.HistoryEnabledDatabases.Any(x => string.Equals(databaseName, x, StringComparison.OrdinalIgnoreCase));
      HistoryDetailsEnabled = HistorySettings.HistoryDetailsEnabledDatabases.Any(x => string.Equals(databaseName, x, StringComparison.OrdinalIgnoreCase));

      DatabaseName = databaseName;
      NextLogTime = DateTime.UtcNow;
      LogInterval = HistorySettings.LogInterval;
      History = new SqlServerHistoryProvider(databaseName);

      if (!HistoryEnabled)
      {
        return;
      }

      Log.Info("Support HistoryLoggingEventQueue is configured for {0} database".FormatWith(databaseName), this);
    }

    [NotNull]
    protected SqlServerHistoryProvider History { get; }

    protected DateTime NextLogTime { get; set; }

    protected virtual bool CanLogCounters
    {
      get
      {
        return NextLogTime <= DateTime.UtcNow;
      }
    }

    public override void ProcessEvents([NotNull] Action<object, Type> handler)
    {
      Assert.ArgumentNotNull(handler, nameof(handler));

      if (!ListenToRemoteEvents)
      {
        return;
      }

      if (!HistoryEnabled)
      {
        base.ProcessEvents(handler);

        return;
      }

      lock (this)
      {
        var mainCount = EventsCount;
        var mainTotal = TotalTime;
        var mainRead = ReadTime;

        mainTotal.Start();
        mainRead.Start();

        var securityDisabler = HistorySettings.SecurityDisabler ? new SecurityDisabler() : null;
        try
        {
          var queuedEvents = GetQueuedEvents(InstanceName);

          DoProcessEvents(handler, queuedEvents, mainRead, mainCount);
        }
        finally
        {
          securityDisabler?.Dispose();

          mainRead.Stop();
          mainTotal.Stop();

          if (CanLogCounters)
          {
            LogAndResetCounters();
          }
        }
      }
    }

    protected virtual void DoProcessEvents([NotNull] Action<object, Type> handler, [NotNull] IEnumerable<QueuedEvent> queuedEvents, [NotNull] Stopwatch mainRead, [NotNull] SimpleCounter mainCount)
    {
      Assert.ArgumentNotNull(handler, nameof(handler));
      Assert.ArgumentNotNull(queuedEvents, nameof(queuedEvents));
      Assert.ArgumentNotNull(mainRead, nameof(mainRead));
      Assert.ArgumentNotNull(mainCount, nameof(mainCount));

      if (!ListenToRemoteEvents)
      {
        return;
      }

      lock (this)
      {
        foreach (var queuedEvent in GetQueuedEvents(InstanceName))
        {
          mainRead.Stop();
          mainCount.Increment();

          DoProcessEvent(handler, queuedEvent);

          mainRead.Start();
        }
      }
    }

    protected virtual void DoProcessEvent([NotNull] Action<object, Type> handler, [NotNull] QueuedEvent queuedEvent)
    {
      Assert.ArgumentNotNull(handler, nameof(handler));
      Assert.ArgumentNotNull(queuedEvent, nameof(queuedEvent));
      if (queuedEvent.EventType == null || queuedEvent.InstanceType == null)
      {
        Log.Warn("Ignoring unknown event: " + queuedEvent.EventTypeName + ", instance: " + queuedEvent.InstanceTypeName + ", sender: " + queuedEvent.InstanceName, this);

        MarkProcessed(queuedEvent);
      }
      else
      {
        DeserializeTime.Start();
        var deserializedEvent = DeserializeEvent(queuedEvent);
        DeserializeTime.Stop();

        ProcessTime.Start();
        handler(deserializedEvent, queuedEvent.InstanceType);
        ProcessTime.Stop();

        MarkProcessed(queuedEvent);

        if (HistoryEnabled && HistoryDetailsEnabled)
        {
          WriteHistory(deserializedEvent, queuedEvent, 0);
        }
      }
    }

    [NotNull]
    protected virtual object DeserializeEvent([NotNull] QueuedEvent queuedEvent)
    {
      Assert.ArgumentNotNull(queuedEvent, nameof(queuedEvent));

      return Serializer.Deserialize(queuedEvent.InstanceData, queuedEvent.InstanceType);
    }

    protected virtual void LogAndResetCounters()
    {
      var queueSize = GetQueuedEventCount();
      var mainCount = EventsCount;
      var mainTotal = TotalTime;
      var mainRead = ReadTime;

      var count = mainCount.Value;
      var totalMs = mainTotal.ElapsedMilliseconds;
      var readMs = mainRead.ElapsedMilliseconds;

      Log.Info($"Health.EventQueue.Size: {queueSize}", this);
      Log.Info($"Health.ReadEQ.Count: {count}", this);
      Log.Info($"Health.ReadEQ.Time.Total: {totalMs}", this);
      Log.Info($"Health.ReadEQ.Time.Read: {readMs}", this);

      if (HistoryEnabled)
      {
        History.AddHistoryEntry("Statistics", "EQ.Size",
          taskStack: queueSize.ToString(),
          userName: string.Empty);

        History.AddHistoryEntry("Statistics", "ReadEQ.Count",
          taskStack: count.ToString(),
          userName: string.Empty);

        History.AddHistoryEntry("Statistics", "ReadEQ.Time.Total",
          taskStack: totalMs.ToString(),
          userName: string.Empty);

        History.AddHistoryEntry("Statistics", "ReadEQ.Time.Read",
          taskStack: readMs.ToString(),
          userName: string.Empty);
      }

      if (count > 0)
      {
        var totalAvg = totalMs / count;
        var readAvg = readMs / count;

        Log.Info($"Health.ReadEQ.Time.Avg.Total: {totalAvg}", this);
        Log.Info($"Health.ReadEQ.Time.Avg.Read: {readAvg}", this);

        if (HistoryEnabled)
        {
          History.AddHistoryEntry("Statistics", "ReadEQ.Time.Avg.Total",
          taskStack: totalAvg.ToString(),
          userName: string.Empty);

          History.AddHistoryEntry("Statistics", "ReadEQ.Time.Avg.Read",
          taskStack: readAvg.ToString(),
          userName: string.Empty);
        }
      }

      LogAndResetProcessCounters();

      mainCount.Reset();
      mainTotal.Reset();
      mainRead.Reset();

      NextLogTime = DateTime.UtcNow + LogInterval;
    }

    protected virtual void LogAndResetProcessCounters()
    {
      var mainCount = EventsCount;
      var mainDeserialize = DeserializeTime;
      var mainProcess = ProcessTime;

      var count = mainCount.Value;
      if (count > 0)
      {
        var deserializeMs = mainDeserialize.ElapsedMilliseconds;
        var processMs = mainProcess.ElapsedMilliseconds;

        Log.Info($"Health.ProcessEQ.Time.Deserialize: {deserializeMs}", this);
        Log.Info($"Health.ProcessEQ.Time.Process: {processMs}", this);

        if (HistoryEnabled)
        {
          History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Deserialize",
          taskStack: deserializeMs.ToString(),
          userName: string.Empty);

          History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Process",
          taskStack: processMs.ToString(),
          userName: string.Empty);
        }

        if (count > 0)
        {
          var deserializeAvg = deserializeMs / count;
          var processAvg = processMs / count;

          Log.Info($"Health.ProcessEQ.Time.Avg.Deserialize: {deserializeAvg}", this);
          Log.Info($"Health.ProcessEQ.Time.Avg.Process: {processAvg}", this);

          if (HistoryEnabled)
          {
            History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Deserialize",
          taskStack: deserializeAvg.ToString(),
          userName: string.Empty);

            History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Process",
          taskStack: processAvg.ToString(),
          userName: string.Empty);
          }
        }
      }

      mainDeserialize.Reset();
      mainProcess.Reset();
    }

    protected void WriteHistory([NotNull] object eventObject, [NotNull] QueuedEvent queuedEvent, int thread)
    {
      Assert.ArgumentNotNull(eventObject, nameof(eventObject));
      Assert.ArgumentNotNull(queuedEvent, nameof(queuedEvent));

      var eventName = queuedEvent.EventType.Name;
      var eventBase = eventObject as ItemRemoteEventBase;
      if (eventBase != null)
      {
        History.AddHistoryEntry("Event", eventName, 
          itemId: ID.Parse(eventBase.ItemId), 
          language: eventBase.LanguageName, 
          version: eventBase.VersionNumber,
          taskStack: thread.ToString(),
          userName: queuedEvent.Created.ToString(DateTimeFormat));

        return;
      }

      var publishEnd = eventObject as PublishEndRemoteEvent;
      if (publishEnd != null)
      {
        History.AddHistoryEntry("Event", eventName, 
          itemId: ID.Parse(publishEnd.RootItemId), 
          language: publishEnd.LanguageName,
          taskStack: thread.ToString(),
          userName: queuedEvent.Created.ToString(DateTimeFormat));

        return;
      }

      History.AddHistoryEntry("Event", eventName,
          taskStack: queuedEvent.Created.ToString(DateTimeFormat));
    }

    public static class HistorySettings
    {
      public static readonly IReadOnlyList<string> HistoryEnabledDatabases = Settings.GetSetting("EventQueue.HistoryLogging.EnabledDatabases", "core|master|web").Split('|');

      public static readonly IReadOnlyList<string> HistoryDetailsEnabledDatabases = Settings.GetSetting("EventQueue.HistoryLogging.DetailsEnabledDatabases", "web").Split('|');

      public static readonly bool SecurityDisabler = Settings.GetBoolSetting("EventQueue.SecurityDisabler", true);

      public static readonly TimeSpan LogInterval = Settings.GetTimeSpanSetting("EventQueue.MainThread.LogInterval", new TimeSpan(0, 0, 5, 0));
    }
  }
}