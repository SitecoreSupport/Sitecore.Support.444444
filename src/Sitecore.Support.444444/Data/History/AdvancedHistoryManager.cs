namespace Sitecore.AdvancedHistory
{
  using Sitecore.Configuration;
  using Sitecore.Diagnostics;

  public static class AdvancedHistoryManager
  {
    [NotNull]
    private static readonly ProviderHelper<AdvancedHistoryProvider, AdvancedHistoryProviderCollection> Helper 
      = new ProviderHelper<AdvancedHistoryProvider, AdvancedHistoryProviderCollection>("advancedHistoryManager");

    [NotNull]
    public static AdvancedHistoryProviderCollection Providers
    {
      get
      {
        return Helper.Providers;
      }
    }

    [NotNull,UsedImplicitly]
    public static AdvancedDatabaseHistoryProvider GetProvider([NotNull] string databaseName)
    {
      Assert.ArgumentNotNull(databaseName, "databaseName");

      var provider = Helper.Provider;
      Assert.IsNotNull(provider, "provider");

      return provider.GetDatabaseHistoryProvider(databaseName);
    }
  }
}