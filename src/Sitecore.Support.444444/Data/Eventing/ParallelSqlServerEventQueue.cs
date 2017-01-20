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

  public abstract class ParallelSqlServerEventQueue : HistoryLoggingEventQueue
  {
    [NotNull]
    protected readonly EventQueue.IPersistentTimestamp EffectivePersistentTimestamp;

    protected readonly bool UseBaseFunctionality;

    private DateTime EffectiveStampLastSaved = DateTime.UtcNow;

    private int queueIndex;

    protected ParallelSqlServerEventQueue([NotNull] SqlDataApi api, [NotNull] Database database)
      : base(api, database)
    {
      Assert.ArgumentNotNull(api, "api");
      Assert.ArgumentNotNull(database, "database");

      var useBaseFunctionality = database.Name != ParallelEventQueueSettings.DatabaseName;
      this.UseBaseFunctionality = useBaseFunctionality;

      Log.Info("ParallelSqlServerEventQueue is {0} for {1}".FormatWith(useBaseFunctionality ? "disabled" : "enabled", database.Name), this);

      this.EffectivePersistentTimestamp = new PrefixedPropertyTimeStamp(database, "EQStampEffective_");
    }

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
        this.queueIndex++;
        if (this.queueIndex >= ParallelEventQueueSettings.ParallelThreadsCount)
        {
          this.queueIndex = 0;
        }

        var queue = this.GetQueue(this.queueIndex);

        queue.Enqueue(new EventHandlerPair(handler, queuedEvent));

        // mark that event was taken to processing
        // reverted #1 to fix #5 EQSTamp_instance = queuedEvent.stamp
        this.MarkProcessed(queuedEvent);
      }
    }

    protected void StartThreads()
    {
      var count = ParallelEventQueueSettings.ParallelThreadsCount;
      Assert.IsTrue(count >= 1, "With EventQueue.ParallelThreadsCount < 1 there is no sense in using ParallelSqlServerEventQueue");

      var processedEvents = new QueuedEvent[count];

      // start threads
      Log.Info("Starting {0} EventQueue threads".FormatWith(count), this);

      for (var threadIndex = 0; threadIndex < count; threadIndex++)
      {
        var queue = this.GetQueue(threadIndex);

        this.StartThread(threadIndex, queue, processedEvents);
      }
    }

    protected void StartThread(int threadIndex, [NotNull] ConcurrentQueue<EventHandlerPair> queue, [NotNull] QueuedEvent[] processedEvents)
    {
      Assert.ArgumentNotNull(queue, "queue");
      Assert.ArgumentNotNull(processedEvents, "processedEvents");
      
      var thread = new Thread(this.DoProcessQueue)
      {
        Name = "EventQueueThread #" + threadIndex,
        IsBackground = true
      };

      thread.Start(new ThreadStartParams(queue, threadIndex, processedEvents));
    }

    [NotNull]
    protected abstract ConcurrentQueue<EventHandlerPair> GetQueue(int threadIndex);

    private void DoProcessQueue([NotNull] object obj)
    {
      Assert.ArgumentNotNull(obj, "obj");

      var par = (ThreadStartParams)obj;
      var threadNumber = par.ThreadNumber;

      SecurityDisabler securityDisabler = null;

      try
      {
        var queue = par.Queue;
        var timeStamps = par.ProcessedEvents;

        var count = 0;
        var publishEndCount = 0;
        var deserialize = new Stopwatch();
        var process = new Stopwatch();
        var publishEnd = new Stopwatch();
        var total = new Stopwatch();

        var threadsCount = ParallelEventQueueSettings.ParallelThreadsCount;
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
                Log.Warn("Ignoring unknown event: " + queuedEvent.EventTypeName + ", instance: " + queuedEvent.InstanceTypeName + ", sender: " + queuedEvent.InstanceName, typeof(ParallelEventQueueWithSeparateQueues));
              }
              else
              {
                timeStamps[threadNumber] = queuedEvent;
                if (eventType == typeof(PublishEndRemoteEvent))
                {
                  publishEndCount++;

                  if (publishEndSync && threadsCount > 1)
                  {
                    // wait for other threads process all events that were before publish:end:remote
                    while (true)
                    {
                      if (!this.WaitRequired(threadNumber, timeStamps))
                      {
                        break;
                      }

                      publishEnd.Start();
                      Thread.Sleep(publishEndSleep);
                      publishEnd.Stop();
                    }
                  }
                }


                deserialize.Start();
                var deserializeEvent = this.DeserializeEvent(queuedEvent);
                deserialize.Stop();

                process.Start();
                pair.Handler(deserializeEvent, queuedEvent.InstanceType);
                process.Stop();

                timeStamps[threadNumber] = queuedEvent;

                this.MarkEffectiveProcessed(queuedEvent);
                if (historyEnabled)
                {
                  this.WriteHistory(deserializeEvent, queuedEvent, threadNumber);
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

          Log.Info(string.Format("Health.ProcessEQ{0}.Count: {1}", threadNumber, count), this);
          Log.Info(string.Format("Health.ProcessEQ{0}.PublishEndCount: {1}", threadNumber, publishEndCount), this);
          Log.Info(string.Format("Health.ProcessEQ{0}.Time.Total: {1}", threadNumber, totalMs), this);
          Log.Info(string.Format("Health.ProcessEQ{0}.Time.Deserialize: {1}", threadNumber, deserializeMs), this);
          Log.Info(string.Format("Health.ProcessEQ{0}.Time.Process: {1}", threadNumber, processMs), this);
          Log.Info(string.Format("Health.ProcessEQ{0}.Time.PublishEndSleep: {1}", threadNumber, publishEndMs), this);

          var taskStack = threadNumber.ToString();
          var historyLogEnabled = historyEnabled;
          if (historyLogEnabled)
          {
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Count", ID.Null, count.ToString(), taskStack: taskStack, userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.PublishEndCount", ID.Null, publishEndCount.ToString(), taskStack: taskStack, userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Total", ID.Null, totalMs.ToString(), taskStack: taskStack, userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Deserialize", ID.Null, deserializeMs.ToString(), taskStack: taskStack, userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Process", ID.Null, processMs.ToString(), taskStack: taskStack, userName: string.Empty);
            this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.PublishEndSleep", ID.Null, publishEndMs.ToString(), taskStack: taskStack, userName: string.Empty);
          }

          if (count > 0)
          {
            var totalAvg = totalMs / count;
            var deserializeAvg = deserializeMs / count;
            var processAvg = processMs / count;
            var publishEndAvg = publishEndMs / count;

            Log.Info(string.Format("Health.ProcessEQ{0}.Time.Avg.Total: {1}", threadNumber, totalAvg), this);
            Log.Info(string.Format("Health.ProcessEQ{0}.Time.Avg.Deserialize: {1}", threadNumber, deserializeAvg), this);
            Log.Info(string.Format("Health.ProcessEQ{0}.Time.Avg.Process: {1}", threadNumber, processAvg), this);
            Log.Info(string.Format("Health.ProcessEQ{0}.Time.Avg.PublishEndSleep: {1}", threadNumber, publishEndAvg), this);

            if (historyLogEnabled)
            {
              this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Total", ID.Null, totalAvg.ToString(), taskStack: taskStack, userName: string.Empty);
              this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Deserialize", ID.Null, deserializeAvg.ToString(), taskStack: taskStack, userName: string.Empty);
              this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.Process", ID.Null, processAvg.ToString(), taskStack: taskStack, userName: string.Empty);
              this.History.AddHistoryEntry("Statistics", "ProcessEQ.Time.Avg.PublishEndSleep", ID.Null, publishEndAvg.ToString(), taskStack: taskStack, userName: string.Empty);
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
        Log.Fatal(string.Format("ParallelEventQueue thread #{0} crashed.", threadNumber), ex, this);
      }
      finally
      {
        if (securityDisabler != null)
        {
          securityDisabler.Dispose();
        }
      }
    }

    private bool WaitRequired(int threadNumber, [NotNull] QueuedEvent[] processedEvents)
    {
      Assert.ArgumentNotNull(processedEvents, "processedEvents");

      var currentEvent = processedEvents[threadNumber];
      Assert.IsNotNull(currentEvent, "currentEvent");

      var currentStamp = currentEvent.Timestamp;
      var lastIndex = processedEvents.Length - 1;

      // check all non-pub:end before current one in the array 
      // [ check, check, check <start , this, ... ]
      for (var index = threadNumber - 1; index >= 0; --index)
      {
        var otherEvent = processedEvents[index];
        if (otherEvent == null)
        {
          continue;
        }

        if (otherEvent.InstanceType == typeof(PublishEndRemoteEvent))
        {
          // okay here, need to check second part of the array
          break;
        }

        var otherEventStamp = otherEvent.Timestamp;
        if (otherEventStamp < currentStamp)
        {
          return true;
        }
      }

      // check all non-pub:end after current one in the array 
      // [ ... , this, check, check, check <start ]
      for (var index = lastIndex; index > threadNumber; --index)
      {
        var otherEvent = processedEvents[index];
        if (otherEvent == null)
        {
          continue;
        }

        if (otherEvent.InstanceType == typeof(PublishEndRemoteEvent))
        {
          // okay here as well
          return false;
        }

        var otherEventStamp = otherEvent.Timestamp;
        if (otherEventStamp < currentStamp)
        {
          return true;
        }
      }

      // if min stamp of others is larger then we do not need to wait
      return false;
    }

    private void MarkEffectiveProcessed([NotNull] QueuedEvent queuedEvent)
    {
      Assert.ArgumentNotNull(queuedEvent, "queuedEvent");

      this.SetTimestampForLastEffectiveProcessing(new TimeStamp(queuedEvent.Timestamp));
    }

    private void SetTimestampForLastEffectiveProcessing([NotNull] EventQueue.TimeStamp currentTimestamp)
    {
      Assert.ArgumentNotNull(currentTimestamp, "currentTimestamp");

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