namespace Sitecore.Support.Data.History
{
  using System;
  using Sitecore.Configuration;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Data.SqlServer;
  using Sitecore.Diagnostics;

  public class SqlServerHistoryProvider 
  {
    [NotNull]
    private static readonly string InstanceName = Settings.InstanceName;

    [NotNull]
    private readonly SqlDataApi DataApi;

    public SqlServerHistoryProvider([NotNull] string databaseName)
    {
      Assert.ArgumentNotNull(databaseName, nameof(databaseName));

      DataApi = new SqlServerDataApi(Settings.GetConnectionString(databaseName));
    }

    public void AddHistoryEntry(string category, string action, ID itemId = null, string language = null, int version = 0, string taskStack = null, string userName = null)
    {      
      Assert.ArgumentNotNull(category, nameof(category));
      Assert.ArgumentNotNull(action, nameof(action));
      Assert.ArgumentNotNull(itemId, nameof(itemId));            

      const string Query = @"
INSERT INTO {0}History{1} 
(
  {0}Id{1}, 
  {0}Category{1}, 
  {0}Action{1}, 
  {0}ItemId{1}, 
  {0}ItemLanguage{1}, 
  {0}ItemVersion{1}, 
  {0}ItemPath{1}, 
  {0}TaskStack{1}, 
  {0}UserName{1}, 
  {0}AdditionalInfo{1}, 
  {0}Created{1}
) 
VALUES 
(  
  {2}id{3}, 
  {2}category{3}, 
  {2}action{3}, 
  {2}itemId{3}, 
  {2}itemLanguage{3}, 
  {2}itemVersion{3}, 
  {2}itemPath{3}, 
  {2}taskStack{3}, 
  {2}userName{3}, 
  {2}additionalInfo{3}, 
  {2}created{3}
)";

      var parameters = new object[]
      {
        "id", ID.NewID,
        "category", category,
        "action", action,
        "itemId", itemId,
        "itemLanguage", language ?? "",
        "itemVersion", version,
        "itemPath", string.Empty,
        "taskStack", taskStack ?? "",
        "userName", userName ?? "",
        "additionalInfo", InstanceName,
        "created", DateTime.UtcNow
      };

      DataApi.Execute(Query, parameters);
    }
  }
}