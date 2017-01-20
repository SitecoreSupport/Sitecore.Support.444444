namespace Sitecore.Support.Data.Eventing
{
  using System;
  using System.Collections.Generic;
  using System.Diagnostics;
  using System.Threading;
  using Sitecore.AdvancedHistory;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Data.Eventing;
  using Sitecore.Data.Eventing.Remote;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;
  using Sitecore.Eventing.Remote;
  using Sitecore.SecurityModel;
  using Sitecore.StringExtensions;
  using Sitecore.Support.Diagnostics;

  [UsedImplicitly]
  public class HistoryLoggingEventQueue : SqlServerEventQueue
  {
    private const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";

    [NotNull]
    protected readonly string DatabaseName;

    protected readonly bool UseBaseFunctionality;

    private readonly AdvancedSetting<TimeSpan> LogInterval;

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
      Assert.ArgumentNotNull(api, "api");
      Assert.ArgumentNotNull(database, "database");

      var databaseName = database.Name;
      var useBaseFunctionality = databaseName != EventQueueSettings.DatabaseName.Value;

      this.UseBaseFunctionality = useBaseFunctionality;
      this.DatabaseName = databaseName;
      this.NextLogTime = DateTime.UtcNow;
      this.LogInterval = EventQueueSettings.LogInterval;

      if (useBaseFunctionality)
      {
        return;
      }

      this.InitializeHistory();
      SettingBasedAdvancedHistoryProvider.Provider.ValueChanged += (x, y) => this.InitializeHistory();

      Log.Info("Support HistoryLoggingEventQueue is configured for {0} database".FormatWith(databaseName), this);
    }

    [NotNull]
    protected AdvancedDatabaseHistoryProvider History { get; private set; }

    protected DateTime NextLogTime { get; set; }

    protected virtual bool CanLogCounters
    {
      get
      {
        return this.NextLogTime <= DateTime.UtcNow;
      }
    }

    public override void ProcessEvents([NotNull] Action<object, Type> handler)
    {
      Assert.ArgumentNotNull(handler, "handler");
      
      if (!this.ListenToRemoteEvents)
      {
        return;
      }

      if (this.UseBaseFunctionality)
      {
        base.ProcessEvents(handler);

        return;
      }

      lock (this)
      {
        var mainCount = this.EventsCount;
        var mainTotal = this.TotalTime;
        var mainRead = this.ReadTime;

        mainTotal.Start();
        mainRead.Start();

        var securityDisabler = EventQueueSettings.SecurityDisabler.Value ? new SecurityDisabler() : null;
        try
        {
          var queuedEvents = this.GetQueuedEvents(this.InstanceName);

          this.DoProcessEvents(handler, queuedEvents, mainRead, mainCount);
        }
        finally
        {
          if (securityDisabler != null)
          {
            securityDisabler.Dispose();
          }

          mainRead.Stop();
          mainTotal.Stop();

          if (this.CanLogCounters)
          {
            this.LogAndResetCounters();
          }
        }
      }
    }

    protected virtual void DoProcessEvents([NotNull] Action<object, Type> handler, [NotNull] IEnumerable<QueuedEvent> queuedEvents, [NotNull] Stopwatch mainRead, [NotNull] SimpleCounter mainCount)
    {
      Assert.ArgumentNotNull(handler, "handler");
      Assert.ArgumentNotNull(queuedEvents, "queuedEvents");
      Assert.ArgumentNotNull(mainRead, "mainRead");
      Assert.ArgumentNotNull(mainCount, "mainCount");

      if (!this.ListenToRemoteEvents)
      {
        return;
      }

      lock (this)
      {
        foreach (var queuedEvent in this.GetQueuedEvents(this.InstanceName))
        {
          mainRead.Stop();
          mainCount.Increment();

          this.DoProcessEvent(handler, queuedEvent);

          mainRead.Start();
        }
      }
    }

    protected virtual void DoProcessEvent([NotNull] Action<object, Type> handler, [NotNull] QueuedEvent queuedEvent)
    {
      Assert.ArgumentNotNull(handler, "handler");
      Assert.ArgumentNotNull(queuedEvent, "queuedEvent");
      if (queuedEvent.EventType == null || queuedEvent.InstanceType == null)
      {
        Log.Warn("Ignoring unknown event: " + queuedEvent.EventTypeName + ", instance: " + queuedEvent.InstanceTypeName + ", sender: " + queuedEvent.InstanceName, this);

        this.MarkProcessed(queuedEvent);
      }
      else
      {
        this.DeserializeTime.Start();
        var deserializedEvent = this.DeserializeEvent(queuedEvent);
        this.DeserializeTime.Stop();

        this.ProcessTime.Start();
        handler(deserializedEvent, queuedEvent.InstanceType);
        this.ProcessTime.Stop();

        this.MarkProcessed(queuedEvent);

        if (EventQueueSettings.HistoryEnabled.Value && EventQueueSettings.HistoryDetailsEnabled.Value)
        {
          this.WriteHistory(deserializedEvent, queuedEvent, 0);
        }
      }
    }

    [NotNull]
    protected virtual object DeserializeEvent([NotNull] QueuedEvent queuedEvent)
    {
      Assert.ArgumentNotNull(queuedEvent, "queuedEvent");

      return this.Serializer.Deserialize(queuedEvent.InstanceData, queuedEvent.InstanceType);
    }

    protected virtual void LogAndResetCounters()
    {
      var queueSize = this.GetQueuedEventCount();
      var mainCount = this.EventsCount;
      var mainTotal = this.TotalTime;
      var mainRead = this.ReadTime;

      var count = mainCount.Value;
      var totalMs = mainTotal.ElapsedMilliseconds;
      var readMs = mainRead.ElapsedMilliseconds;

      Log.Info(string.Format("Health.EventQueue.Size: {0}", queueSize), this);
      Log.Info(string.Format("Health.ReadEQ.Count: {0}", count), this);
      Log.Info(string.Format("Health.ReadEQ.Time.Total: {0}", totalMs), this);
      Log.Info(string.Format("Health.ReadEQ.Time.Read: {0}", readMs), this);

      var historyLogEnabled = EventQueueSettings.HistoryEnabled.Value;
      if (historyLogEnabled)
      {
        this.History.AddHistoryEntry("Statistics", "EQ.Size", ID.Null, queueSize.ToString(), userName: string.Empty);
        this.History.AddHistoryEntry("Statistics", "ReadEQ.Count", ID.Null, count.ToString(), userName: string.Empty);
        this.History.AddHistoryEntry("Statistics", "ReadEQ.Time.Total", ID.Null, totalMs.ToString(), userName: string.Empty);
        this.History.AddHistoryEntry("Statistics", "ReadEQ.Time.Read", ID.Null, readMs.ToString(), userName: string.Empty);
      }

      if (count > 0)
      {
        var totalAvg = totalMs / count;
        var readAvg = readMs / count;

        Log.Info(string.Format("Health.ReadEQ.Time.Avg.Total: {0}", totalAvg), this);
        Log.Info(string.Format("Health.ReadEQ.Time.Avg.Read: {0}", readAvg), this);

        if (historyLogEnabled)
        {
          this.History.AddHistoryEntry("Statistics", "ReadEQ.Time.Avg.Total", ID.Null, totalAvg.ToString(), userName: string.Empty);
          this.History.AddHistoryEntry("Statistics", "ReadEQ.Time.Avg.Read", ID.Null, readAvg.ToString(), userName: string.Empty);
        }
      }

      this.LogAndResetProcessCounters();

      mainCount.Reset();
      mainTotal.Reset();
      mainRead.Reset();

      this.NextLogTime = DateTime.UtcNow + this.LogInterval.Value;
    }

    protected virtual void LogAndResetProcessCounters()
    {
      var mainCount = this.EventsCount;
      var mainDeserialize = this.DeserializeTime;
      var mainProcess = this.ProcessTime;

      var count = mainCount.Value;
      if (count > 0)
      {
        var deserializeMs = mainDeserialize.ElapsedMilliseconds;
        var processMs = mainProcess.ElapsedMilliseconds;

        Log.Info(string.Format("Health.ProcessEQ.Time.Deserialize: {0}", deserializeMs), this);
        Log.Info(string.Format("Health.ProcessEQ.Time.Process: {0}", processMs), this);

        var historyLogEnabled = EventQueueSettings.HistoryEnabled.Value;
        if (historyLogEnabled)
        {
          this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Deserialize", ID.Null, deserializeMs.ToString(), userName: string.Empty);
          this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Process", ID.Null, processMs.ToString(), userName: string.Empty);
        }

        if (count > 0)
        {
          var deserializeAvg = deserializeMs / count;
          var processAvg = processMs / count;

          Log.Info(string.Format("Health.ProcessEQ.Time.Avg.Deserialize: {0}", deserializeAvg), this);
          Log.Info(string.Format("Health.ProcessEQ.Time.Avg.Process: {0}", processAvg), this);

          if (historyLogEnabled)
          {
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Deserialize", ID.Null, deserializeAvg.ToString(), userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Process", ID.Null, processAvg.ToString(), userName: string.Empty);
          }
        }
      }

      mainDeserialize.Reset();
      mainProcess.Reset();
    }

    protected void WriteHistory([NotNull] object eventObject, [NotNull] QueuedEvent queuedEvent, int threadNumber)
    {
      Assert.ArgumentNotNull(eventObject, "eventObject");
      Assert.ArgumentNotNull(queuedEvent, "queuedEvent");

      var eventName = queuedEvent.EventType.Name;
      var eventBase = eventObject as ItemRemoteEventBase;
      if (eventBase != null)
      {
        this.History.AddHistoryEntry("Event", eventName, ID.Parse(eventBase.ItemId), eventBase.LanguageName, eventBase.VersionNumber, taskStack: threadNumber.ToString(), userName: queuedEvent.Created.ToString(DateTimeFormat));

        return;
      }

      var publishEnd = eventObject as PublishEndRemoteEvent;
      if (publishEnd != null)
      {
        this.History.AddHistoryEntry("Event", eventName, ID.Parse(publishEnd.RootItemId), publishEnd.LanguageName, taskStack: threadNumber.ToString(), userName: queuedEvent.Created.ToString(DateTimeFormat));

        return;
      }

      this.History.AddHistoryEntry("Event", eventName, ID.Null, taskStack: threadNumber.ToString(), userName: queuedEvent.Created.ToString(DateTimeFormat));
    }

    private void InitializeHistory()
    {
      this.History = AdvancedHistoryManager.GetProvider(this.DatabaseName);
    }
  }
}