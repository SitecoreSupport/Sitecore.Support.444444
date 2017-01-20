namespace Sitecore.Support.Data.Eventing
{
  using System;
  using Sitecore.Configuration;

  public static class EventQueueSettings
  {
    public static readonly string DatabaseName = Settings.GetSetting("EventQueue.DatabaseName", "web");

    public static readonly bool HistoryEnabled = Settings.GetBoolSetting("EventQueue.HistoryEnabled", true);

    public static readonly bool HistoryDetailsEnabled = Settings.GetBoolSetting("EventQueue.HistoryDetailsEnabled", true);

    public static readonly bool SecurityDisabler = Settings.GetBoolSetting("EventQueue.SecurityDisabler", true);

    public static readonly TimeSpan LogInterval = Settings.GetTimeSpanSetting("EventQueue.MainThread.LogInterval", new TimeSpan(0, 0, 5, 0));
  }
}