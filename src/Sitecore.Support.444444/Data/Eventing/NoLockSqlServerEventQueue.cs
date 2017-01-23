namespace Sitecore.Support.Data.Eventing
{
  using System.Linq;
  using Sitecore.Configuration;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Data.Eventing;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;

  public class NoLockSqlServerEventQueue : SqlServerEventQueue
  {
    public static readonly bool NoLockEnabled = NoLockProcessingSettings.Enabled;

    [UsedImplicitly]
    public NoLockSqlServerEventQueue(SqlDataApi api, Database database) : base(api, database)
    {
      Assert.ArgumentNotNull(api, "api");
      Assert.ArgumentNotNull(database, "database");
    }

    public override long GetQueuedEventCount()
    {
      if (!NoLockEnabled)
      {
        return base.GetQueuedEventCount();
      }

      return DataApi.CreateObjectReader("SELECT COUNT(*) FROM {0}EventQueue{1} WITH (NOLOCK)", new object[0], r => GetLong(r, 0))
        .FirstOrDefault();
    }

    protected override SqlStatement GetSqlStatement(EventQueueQuery query)
    {
      Assert.ArgumentNotNull(query, nameof(query));

      if (!NoLockEnabled)
      {
        return base.GetSqlStatement(query);
      }

      var statement = new SqlStatement
      {
        Select = "SELECT {0}EventType{1}, {0}InstanceType{1}, {0}InstanceData{1}, {0}InstanceName{1}, {0}UserName{1}, {0}Stamp{1}, {0}Created{1}",
        From = "FROM {0}EventQueue{1} WITH (NOLOCK)",
        OrderBy = "ORDER BY {0}Stamp{1}"
      };

      AddCriteria(statement, query);

      return statement;
    }
  }

  public static class NoLockProcessingSettings
  {
    public static readonly bool Enabled = Settings.GetBoolSetting("EventQueue.NoLockProcessing.Enabled", true);
  }
}