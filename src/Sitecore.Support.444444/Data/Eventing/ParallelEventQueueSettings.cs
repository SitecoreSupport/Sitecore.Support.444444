namespace Sitecore.Support.Data.Eventing
{
  using System;

  public static class ParallelEventQueueSettings
  {
    [NotNull]
    public static readonly AdvancedSetting<string> DatabaseName = AdvancedSettings.Create("ParallelEventQueue.DatabaseName", "web");

    [NotNull]
    public static readonly AdvancedSetting<int> ParallelThreadsCount = AdvancedSettings.Create("ParallelEventQueue.ParallelThreadsCount", 4);

    [NotNull]
    public static readonly AdvancedSetting<int> EventQueueThreadDeepSleep = AdvancedSettings.Create("ParallelEventQueue.EventQueueThread.DeepSleep", 1000);

    [NotNull]
    public static readonly AdvancedSetting<int> EventQueueThreadBatchSize = AdvancedSettings.Create("ParallelEventQueue.EventQueueThread.BatchSize", 1000);

    [NotNull]
    public static readonly AdvancedSetting<TimeSpan> EventQueueThreadLogInterval = AdvancedSettings.Create("ParallelEventQueue.EventQueueThread.LogInterval", new TimeSpan(0, 0, 5, 0));

    [NotNull]
    public static readonly AdvancedSetting<int> EventQueueThreadPublishEndSleep = AdvancedSettings.Create("ParallelEventQueue.EventQueueThread.PublishEndSleep", 1000);

    [NotNull]
    public static readonly AdvancedSetting<bool> EventQueueThreadPublishEndSynchronization = AdvancedSettings.Create("ParallelEventQueue.EventQueueThread.PublishEndSynchronization", true);
  }
}