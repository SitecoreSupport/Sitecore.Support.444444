namespace Sitecore.Support.Data.Eventing
{
  using System;
  using Sitecore.Configuration;

  public static class ParallelEventQueueSettings
  {
    [NotNull]
    public static readonly string DatabaseName = Settings.GetSetting("ParallelEventQueue.DatabaseName", "web");

    [NotNull]
    public static readonly int ParallelThreadsCount = Settings.GetIntSetting("ParallelEventQueue.ParallelThreadsCount", 4);

    [NotNull]
    public static readonly int EventQueueThreadDeepSleep = Settings.GetIntSetting("ParallelEventQueue.EventQueueThread.DeepSleep", 1000);

    [NotNull]
    public static readonly int EventQueueThreadBatchSize = Settings.GetIntSetting("ParallelEventQueue.EventQueueThread.BatchSize", 1000);

    [NotNull]
    public static readonly TimeSpan EventQueueThreadLogInterval = Settings.GetTimeSpanSetting("ParallelEventQueue.EventQueueThread.LogInterval", new TimeSpan(0, 0, 5, 0));

    [NotNull]
    public static readonly int EventQueueThreadPublishEndSleep = Settings.GetIntSetting("ParallelEventQueue.EventQueueThread.PublishEndSleep", 1000);

    [NotNull]
    public static readonly bool EventQueueThreadPublishEndSynchronization = Settings.GetBoolSetting("ParallelEventQueue.EventQueueThread.PublishEndSynchronization", true);
  }
}