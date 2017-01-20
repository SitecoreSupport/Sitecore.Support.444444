namespace Sitecore.AdvancedHistory
{
  using System.Configuration.Provider;

  public class AdvancedHistoryProviderCollection : ProviderCollection
  {
    public new AdvancedHistoryProvider this[string name]
    {
      get
      {
        return base[name] as AdvancedHistoryProvider;
      }
    }
  }
}