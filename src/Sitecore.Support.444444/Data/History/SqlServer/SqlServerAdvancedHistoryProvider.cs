namespace Sitecore.AdvancedHistory.SqlServer
{
  [UsedImplicitly]
  public class SqlServerAdvancedHistoryProvider : AdvancedHistoryProvider
  {
    [CanBeNull]
    private SqlServerAdvancedDatabaseHistoryProvider innerProvider;

    [NotNull]
    public override AdvancedDatabaseHistoryProvider GetDatabaseHistoryProvider([CanBeNull] string databaseName)
    {
      return this.innerProvider ?? (this.innerProvider = this.CreateInnerProvider());
    }

    [NotNull]
    private SqlServerAdvancedDatabaseHistoryProvider CreateInnerProvider()
    {
      lock (this)
      {
        return this.innerProvider ?? new SqlServerAdvancedDatabaseHistoryProvider(this.DatabaseName);
      }
    }
  }
}