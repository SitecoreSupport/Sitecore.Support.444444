namespace Sitecore.AdvancedHistory
{
  using Sitecore.Diagnostics;

  [UsedImplicitly]
  public class SettingBasedAdvancedHistoryProvider : AdvancedHistoryProvider
  {
    public static readonly AdvancedSetting<string> Provider = AdvancedSettings.Create("AdvancedHistory.Provider", "sql");
    
    public override AdvancedDatabaseHistoryProvider GetDatabaseHistoryProvider(string databaseName)
    {
      Assert.ArgumentNotNull(databaseName, "databaseName");

      var provider = this.GetProvider(databaseName);
      if (provider == null)
      {
        return null;
      }

      return provider.GetDatabaseHistoryProvider(databaseName);
    }

    [CanBeNull]
    protected virtual AdvancedHistoryProvider GetProvider([NotNull] string databaseName)
    {
      Assert.ArgumentNotNull(databaseName, "databaseName");

      return AdvancedHistoryManager.Providers[Provider.Value];
    }
  }
}