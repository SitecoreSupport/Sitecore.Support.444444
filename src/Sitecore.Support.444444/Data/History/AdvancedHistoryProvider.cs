namespace Sitecore.AdvancedHistory
{
  using System;
  using System.Collections.Generic;
  using System.Collections.Specialized;
  using System.Configuration.Provider;
  using Sitecore.Diagnostics;

  public abstract class AdvancedHistoryProvider : ProviderBase
  {
    public override void Initialize([CanBeNull] string name, NameValueCollection config)
    {
      Assert.ArgumentNotNull(config, "config");

      var database = config["database"];
      Assert.IsNotNull(database, "database");

      this.DatabaseName = database;

      base.Initialize(name, config);
    }

    [CanBeNull]
    public string DatabaseName { get; private set; }
    
    [CanBeNull]
    public abstract AdvancedDatabaseHistoryProvider GetDatabaseHistoryProvider([NotNull] string databaseName);
  }
}