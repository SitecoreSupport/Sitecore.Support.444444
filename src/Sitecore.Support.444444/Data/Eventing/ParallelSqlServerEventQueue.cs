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
      Assert.ArgumentNotNull(api, nameof(api));
      Assert.ArgumentNotNull(database, nameof(database));

      var useBaseFunctionality = database.Name != ParallelEventQueueSettings.DatabaseName;
      UseBaseFunctionality = useBaseFunctionality;

      Log.Info("ParallelSqlServerEventQueue is {0} for {1}".FormatWith(useBaseFunctionality ? "disabled" : "enabled", database.Name), this);

      EffectivePersistentTimestamp = new PrefixedPropertyTimeStamp(database, "EQStampEffective_");

      if (UseBaseFunctionality)
      {
        return;
      }

      StartThread();
    }

    [NotNull]
    protected ConcurrentQueue<EventHandlerPair> Queue { get; } = new ConcurrentQueue<EventHandlerPair>();

    /// <summary>
    /// Puts the event into in-memory queue and marks it "processed".
    /// </summary>
    protected override void DoProcessEvent([NotNull] Action<object, Type> handler, [NotNull] QueuedEvent queuedEvent)
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
        Queue.Enqueue(new EventHandlerPair(handler, queuedEvent));

        // mark that event was taken to processing
        // reverted #1 to fix #5 EQSTamp_instance = queuedEvent.stamp
        MarkProcessed(queuedEvent);
      }
    }

    protected void StartThread()
    {
      // start threads
      Log.Info("Starting EventQueue background thread", this);

      var thread = new Thread(DoProcessQueue)
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
        var logInterval = ParallelEventQueueSettings.EventQueueThreadLogInterval;
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
                var deserializeEvent = DeserializeEvent(queuedEvent);
                deserialize.Stop();

                process.Start();
                pair.Handler(deserializeEvent, queuedEvent.InstanceType);
                process.Stop();

                MarkEffectiveProcessed(queuedEvent);
                if (HistoryEnabled)
                {
                  WriteHistory(deserializeEvent, queuedEvent, 1);
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

          Log.Info($"Health.ProcessEQ.Count: {count}", this);
          Log.Info($"Health.ProcessEQ.PublishEndCount: {publishEndCount}", this);
          Log.Info($"Health.ProcessEQ.Time.Total: {totalMs}", this);
          Log.Info($"Health.ProcessEQ.Time.Deserialize: {deserializeMs}", this);
          Log.Info($"Health.ProcessEQ.Time.Process: {processMs}", this);
          Log.Info($"Health.ProcessEQ.Time.PublishEndSleep: {publishEndMs}", this);

          if (HistoryEnabled)
          {
            History.AddHistoryEntry("Statistics", "ProcessEQ.Count",
              taskStack: count.ToString());
            History.AddHistoryEntry("Statistics", "ProcessEQ.PublishEndCount",
              taskStack: publishEndCount.ToString());
            History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Total",
              taskStack: totalMs.ToString());
            History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Deserialize",
              taskStack: deserializeMs.ToString());
            History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Process",
              taskStack: processMs.ToString());
            History.AddHistoryEntry("Statistics", "ProcessEQ.Time.PublishEndSleep",
              taskStack: publishEndMs.ToString());
          }

          if (count > 0)
          {
            var totalAvg = totalMs / count;
            var deserializeAvg = deserializeMs / count;
            var processAvg = processMs / count;
            var publishEndAvg = publishEndMs / count;

            Log.Info($"Health.ProcessEQ.Time.Avg.Total: {totalAvg}", this);
            Log.Info($"Health.ProcessEQ.Time.Avg.Deserialize: {deserializeAvg}", this);
            Log.Info($"Health.ProcessEQ.Time.Avg.Process: {processAvg}", this);
            Log.Info($"Health.ProcessEQ.Time.Avg.PublishEndSleep: {publishEndAvg}", this);

            if (HistoryEnabled)
            {
              History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Total",
              taskStack: totalAvg.ToString());
              History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Deserialize",
              taskStack: deserializeAvg.ToString());
              History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Process",
              taskStack: processAvg.ToString());
              History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.PublishEndSleep",
              taskStack: publishEndAvg.ToString());
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
      Assert.ArgumentNotNull(queuedEvent, nameof(queuedEvent));

      SetTimestampForLastEffectiveProcessing(new TimeStamp(queuedEvent.Timestamp));
    }

    private void SetTimestampForLastEffectiveProcessing([NotNull] EventQueue.TimeStamp currentTimestamp)
    {
      Assert.ArgumentNotNull(currentTimestamp, nameof(currentTimestamp));

      if (!(DateTime.UtcNow - EffectiveStampLastSaved > Settings.EventQueue.PersistStampInterval))
      {
        return;
      }

      EffectivePersistentTimestamp.SaveTimestamp(currentTimestamp);
      EffectiveStampLastSaved = DateTime.UtcNow;
    }

    private class PrefixedPropertyTimeStamp : EventQueue.IPersistentTimestamp
    {
      [NotNull]
      private readonly Database database;

      [NotNull]
      private readonly string Prefix;

      public PrefixedPropertyTimeStamp([NotNull] Database database, [NotNull] string prefix)
      {
        Assert.ArgumentNotNull(database, nameof(database));
        Assert.ArgumentNotNull(prefix, nameof(prefix));

        this.database = database;
        Prefix = prefix;
      }

      [CanBeNull]
      public EventQueue.TimeStamp RetrieveTimestamp()
      {
        string str = database.Properties[Prefix + Settings.InstanceName];
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
        Assert.ArgumentNotNull(value, nameof(value));

        database.Properties[Prefix + Settings.InstanceName] = value.ToString();
      }
    }
  }
}