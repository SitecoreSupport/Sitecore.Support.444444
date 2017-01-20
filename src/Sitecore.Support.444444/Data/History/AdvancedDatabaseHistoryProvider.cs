namespace Sitecore.AdvancedHistory
{
  using Sitecore.Data;

  public abstract class AdvancedDatabaseHistoryProvider
  {
    public abstract void AddHistoryEntry([NotNull] string category, [NotNull] string action, [NotNull] ID itemId, [CanBeNull] string language = null, int version = 0, [CanBeNull] string taskStack = null, [CanBeNull] string userName = null);    
  }
}