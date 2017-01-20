namespace Sitecore
{
  using Sitecore.Configuration;
  using Sitecore.Diagnostics;
  using Sitecore.Events.Hooks;
  using Sitecore.Services;
  using Sitecore.StringExtensions;

  [UsedImplicitly]
  public class AdvancedSettingsInitializeHook : IHook
  {
    [NotNull]
    private static readonly AlarmClock AlarmClock = new AlarmClock(Settings.GetTimeSpanSetting("AdvancedSettings.PollingInterval", "00:00:10"));

    public void Initialize()
    {
      Log.Info("AdvancedSettings manager is initialized for {0} database with {1} interval".FormatWith(AdvancedSettings.SystemDatabase, AlarmClock.Interval), typeof(AdvancedSettings));

      AdvancedSettings.Poll(this, null);
      AlarmClock.Ring += AdvancedSettings.Poll;
    }
  }
}