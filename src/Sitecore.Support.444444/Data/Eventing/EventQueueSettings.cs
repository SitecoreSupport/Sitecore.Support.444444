namespace Sitecore.Support.Data.Eventing
{
  using System;
  using System.Collections.Generic;
  using Sitecore.Configuration;

  public static class EventQueueSettings
  {                                                                                                                        
    public static readonly IReadOnlyList<string> HistoryEnabledDatabases = Settings.GetSetting("EventQueue.HistoryEnabledDatabases", "core|master|web").Split('|');

    public static readonly IReadOnlyList<string> HistoryDetailsEnabledDatabases = Settings.GetSetting("EventQueue.HistoryDetailsEnabledDatabases", "web").Split('|');

    public static readonly bool SecurityDisabler = Settings.GetBoolSetting("EventQueue.SecurityDisabler", true);

    public static readonly TimeSpan LogInterval = Settings.GetTimeSpanSetting("EventQueue.MainThread.LogInterval", new TimeSpan(0, 0, 5, 0));
  }
}