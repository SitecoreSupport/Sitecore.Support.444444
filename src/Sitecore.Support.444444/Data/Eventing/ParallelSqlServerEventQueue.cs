namespace Sitecore.Support.Data.Eventing
{
  using System;
  using System.Collections.Concurrent;
  using System.Diagnostics;
  using System.Threading;
  using Sitecore.Configuration;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;
  using Sitecore.Eventing.Remote;
  using Sitecore.SecurityModel;
  using Sitecore.StringExtensions;
  using Sitecore.Support.Data.ParallelEventQueue;

  public class ParallelSqlServerEventQueue : HistoryLoggingEventQueue
  {
    [NotNull]
    protected readonly EventQueue.IPersistentTimestamp EffectivePersistentTimestamp;

    protected readonly bool UseBaseFunctionality;

    private DateTime EffectiveStampLastSaved = DateTime.UtcNow;

    public ParallelSqlServerEventQueue([NotNull] SqlDataApi api, [NotNull] Database database)
      : base(api, database)
    {
      Assert.ArgumentNotNull(api, "api");
      Assert.ArgumentNotNull(database, "database");

      var useBaseFunctionality = database.Name != ParallelEventQueueSettings.DatabaseName;
      this.UseBaseFunctionality = useBaseFunctionality;

      Log.Info("ParallelSqlServerEventQueue is {0} for {1}".FormatWith(useBaseFunctionality ? "disabled" : "enabled", database.Name), this);

      this.EffectivePersistentTimestamp = new PrefixedPropertyTimeStamp(database, "EQStampEffective_");
             
      if (this.UseBaseFunctionality)
      {
        return;
      }

      this.StartThread();
    }

    [NotNull]
    protected ConcurrentQueue<EventHandlerPair> Queue { get; } = new ConcurrentQueue<EventHandlerPair>();

    /// <summary>
    /// Puts the event into in-memory queue and marks it "processed".
    /// </summary>
    protected override void DoProcessEvent([NotNull] Action<object, Type> handler, [NotNull] QueuedEvent queuedEvent)
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
        Queue.Enqueue(new EventHandlerPair(handler, queuedEvent));

        // mark that event was taken to processing
        // reverted #1 to fix #5 EQSTamp_instance = queuedEvent.stamp
        this.MarkProcessed(queuedEvent);
      }
    }

    protected void StartThread()
    {            
      // start threads
      Log.Info("Starting EventQueue background thread", this);         
      
      var thread = new Thread(this.DoProcessQueue)
      {
        Name = "EventQueueThread",
        IsBackground = true
      };

      thread.Start(Queue);
    }                                                                           

    private void DoProcessQueue([NotNull] object obj)
    {
      Assert.ArgumentNotNull(obj, nameof(obj));

      var queue = (ConcurrentQueue<EventHandlerPair>)obj;
      
      SecurityDisabler securityDisabler = null;

      try
      {
        var count = 0;
        var publishEndCount = 0;
        var deserialize = new Stopwatch();
        var process = new Stopwatch();
        var publishEnd = new Stopwatch();
        var total = new Stopwatch();

        var deepSleep = ParallelEventQueueSettings.EventQueueThreadDeepSleep;
        var securityDisablerEnabled = EventQueueSettings.SecurityDisabler;
        var publishEndSleep = ParallelEventQueueSettings.EventQueueThreadPublishEndSleep;
        var publishEndSync = ParallelEventQueueSettings.EventQueueThreadPublishEndSynchronization;
        var logInterval = ParallelEventQueueSettings.EventQueueThreadLogInterval;
        var historyEnabled = EventQueueSettings.HistoryEnabled;
        var nextLogTime = DateTime.UtcNow;

        while (true)
        {
          total.Start();
          if (securityDisablerEnabled && securityDisabler == null)
          {
            securityDisabler = new SecurityDisabler();
          }
          else if (securityDisabler != null)
          {
            securityDisabler.Dispose();
            securityDisabler = null;
          }

          if (queue.IsEmpty)
          {
            total.Stop();
            Thread.Sleep(deepSleep);
          }
          else
          {
            for (var i = 0; i < ParallelEventQueueSettings.EventQueueThreadBatchSize; ++i)
            {
              EventHandlerPair pair;
              if (!queue.TryDequeue(out pair))
              {
                continue;
              }

              count++;
              var queuedEvent = pair.QueuedEvent;
              var eventType = queuedEvent.EventType;
              if (eventType == null || queuedEvent.InstanceType == null)
              {
                Log.Warn("Ignoring unknown event: " + queuedEvent.EventTypeName + ", instance: " + queuedEvent.InstanceTypeName + ", sender: " + queuedEvent.InstanceName, typeof(ParallelSqlServerEventQueue));
              }
              else
              {
                if (eventType == typeof(PublishEndRemoteEvent))
                {
                  publishEndCount++;       
                }    

                deserialize.Start();
                var deserializeEvent = this.DeserializeEvent(queuedEvent);
                deserialize.Stop();

                process.Start();
                pair.Handler(deserializeEvent, queuedEvent.InstanceType);
                process.Stop();                         

                this.MarkEffectiveProcessed(queuedEvent);
                if (historyEnabled)
                {
                  this.WriteHistory(deserializeEvent, queuedEvent, 1);
                }
              }
            }
          }

          total.Stop();

          if (nextLogTime > DateTime.UtcNow)
          {
            continue;
          }

          var totalMs = total.ElapsedMilliseconds;
          var deserializeMs = deserialize.ElapsedMilliseconds;
          var processMs = process.ElapsedMilliseconds;
          var publishEndMs = publishEnd.ElapsedMilliseconds;

          Log.Info(string.Format("Health.ProcessEQ.Count: {0}", count), this);
          Log.Info(string.Format("Health.ProcessEQ.PublishEndCount: {0}", publishEndCount), this);
          Log.Info(string.Format("Health.ProcessEQ.Time.Total: {0}", totalMs), this);
          Log.Info(string.Format("Health.ProcessEQ.Time.Deserialize: {0}", deserializeMs), this);
          Log.Info(string.Format("Health.ProcessEQ.Time.Process: {0}", processMs), this);
          Log.Info(string.Format("Health.ProcessEQ.Time.PublishEndSleep: {0}", publishEndMs), this);
                                                     
          var historyLogEnabled = historyEnabled;
          if (historyLogEnabled)
          {
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Count", ID.Null, count.ToString(), userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.PublishEndCount", ID.Null, publishEndCount.ToString(), userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Total", ID.Null, totalMs.ToString(), userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Deserialize", ID.Null, deserializeMs.ToString(), userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Process", ID.Null, processMs.ToString(), userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.PublishEndSleep", ID.Null, publishEndMs.ToString(), userName: string.Empty);
          }

          if (count > 0)
          {
            var totalAvg = totalMs / count;
            var deserializeAvg = deserializeMs / count;
            var processAvg = processMs / count;
            var publishEndAvg = publishEndMs / count;

            Log.Info(string.Format("Health.ProcessEQ.Time.Avg.Total: {0}", totalAvg), this);
            Log.Info(string.Format("Health.ProcessEQ.Time.Avg.Deserialize: {0}", deserializeAvg), this);
            Log.Info(string.Format("Health.ProcessEQ.Time.Avg.Process: {0}", processAvg), this);
            Log.Info(string.Format("Health.ProcessEQ.Time.Avg.PublishEndSleep: {0}", publishEndAvg), this);

            if (historyLogEnabled)
            {
              this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Total", ID.Null, totalAvg.ToString(), userName: string.Empty);
              this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Deserialize", ID.Null, deserializeAvg.ToString(), userName: string.Empty);
              this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Process", ID.Null, processAvg.ToString(), userName: string.Empty);
              this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.PublishEndSleep", ID.Null, publishEndAvg.ToString(), userName: string.Empty);
            }
          }

          count = 0;
          publishEndCount = 0;
          total.Reset();
          deserialize.Reset();
          process.Reset();
          publishEnd.Reset();

          nextLogTime = DateTime.UtcNow + logInterval;
        }
      }
      catch (Exception ex)
      {
        Log.Fatal(string.Format("ParallelEventQueue background thread crashed."), ex, this);
      }
      finally
      {
        if (securityDisabler != null)
        {
          securityDisabler.Dispose();
        }
      }
    }

    private void MarkEffectiveProcessed([NotNull] QueuedEvent queuedEvent)
    {
      Assert.ArgumentNotNull(queuedEvent, "queuedEvent");

      this.SetTimestampForLastEffectiveProcessing(new TimeStamp(queuedEvent.Timestamp));
    }

    private void SetTimestampForLastEffectiveProcessing([NotNull] EventQueue.TimeStamp currentTimestamp)
    {
      Assert.ArgumentNotNull(currentTimestamp, nameof(currentTimestamp));

      if (!(DateTime.UtcNow - this.EffectiveStampLastSaved > Settings.EventQueue.PersistStampInterval))
      {
        return;
      }

      this.EffectivePersistentTimestamp.SaveTimestamp(currentTimestamp);
      this.EffectiveStampLastSaved = DateTime.UtcNow;
    }

    private class PrefixedPropertyTimeStamp : EventQueue.IPersistentTimestamp
    {
      [NotNull]
      private readonly Database database;

      [NotNull]
      private readonly string Prefix;

      public PrefixedPropertyTimeStamp([NotNull] Database database, [NotNull] string prefix)
      {
        Assert.ArgumentNotNull(database, "database");
        Assert.ArgumentNotNull(prefix, "prefix");

        this.database = database;
        this.Prefix = prefix;
      }

      [CanBeNull]
      public EventQueue.TimeStamp RetrieveTimestamp()
      {
        string str = this.database.Properties[this.Prefix + Settings.InstanceName];
        if (string.IsNullOrEmpty(str))
        {
          return null;
        }

        var timeStamp = TimeStamp.Parse(str);
        if (timeStamp == null)
        {
          return null;
        }

        return new TimeStamp(DateTime.UtcNow - Settings.EventQueue.PersistStampMaxAge, timeStamp.Sequence);
      }

      public void SaveTimestamp([NotNull] EventQueue.TimeStamp value)
      {
        Assert.ArgumentNotNull(value, "value");

        this.database.Properties[this.Prefix + Settings.InstanceName] = value.ToString();
      }
    }
  }
}