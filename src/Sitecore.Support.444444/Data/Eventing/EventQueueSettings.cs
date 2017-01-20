namespace Sitecore.Support.Data.Eventing
{
  using System;

  public static class EventQueueSettings
  {
    public static readonly AdvancedSetting<string> DatabaseName = AdvancedSettings.Create("EventQueue.DatabaseName", "web");

    public static readonly AdvancedSetting<bool> HistoryEnabled = AdvancedSettings.Create("EventQueue.HistoryEnabled", true);

    public static readonly AdvancedSetting<bool> HistoryDetailsEnabled = AdvancedSettings.Create("EventQueue.HistoryDetailsEnabled", true);

    public static readonly AdvancedSetting<bool> SecurityDisabler = AdvancedSettings.Create("EventQueue.SecurityDisabler", false);

    public static readonly AdvancedSetting<TimeSpan> LogInterval = AdvancedSettings.Create("EventQueue.MainThread.LogInterval", new TimeSpan(0, 0, 5, 0));
  }
}