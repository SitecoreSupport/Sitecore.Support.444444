namespace Sitecore.Support.Data.SqlServer
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using Sitecore;
  using Sitecore.Collections;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Diagnostics;
  using Sitecore.StringExtensions;

  public class ViewBasedSqlServerDataProvider : Sitecore.Support.Data.SqlServer.SqlServerDataProvider
  {
    #region Constructors
    //public ViewBasedSqlServerDataProvider(string connectionString) : base(connectionString)
    //{
    //}

    public ViewBasedSqlServerDataProvider(string connectionString, string eventQueueType, string publishQueueProviderType, string databasePropertiesProviderType) : base(connectionString, eventQueueType, publishQueueProviderType, databasePropertiesProviderType)
    {
    }

    protected override void LoadItemDefinitions(string condition, object[] parameters, SafeDictionary<ID, PrefetchData> prefetchData)
    {
      if (condition.Equals(" WHERE {0}ID{1} = {2}itemId{3}"))
      {
        this.GetItemBasedOnView(parameters, prefetchData);
      }
      else
      {
        base.LoadItemDefinitions(condition, parameters, prefetchData);
      }

    }

    #endregion
    /*
     * General layout returned by GetItemView ( rows are in chaos order )
     *                                  READER Columns
     *                0         1           2           3           4          5             6
     * ItemRow        0     |  empty  |  ItemName  |  empty  | TemplateId |  MasterID  |  ParentID  |
     * ChildRow       0     |  empty  |    null    |  empty  |    null    |    null    |  ChildID   |
     * FieldRow     version | language|    null    |  value  |   FieldID  |    null    |    null    |
     */

    private void GetItemBasedOnView([NotNull] object[] parameters, [NotNull] SafeDictionary<ID, PrefetchData> prefetchData)
    {
      Assert.ArgumentNotNull(prefetchData, "prefetchData");

      var itemId = parameters[1] as ID;

      Assert.IsNotNull(itemId, "ItemId fetch failed");

      PrefetchData itemDefPrefetchModel = null;
      List<ID> children = null;

      // language, version, fldId, FldValue
      List<Tuple<string, int, ID, string>> fields = null;

      const string GetItemFromViewUnsorted = "SELECT [Version], [Language], [Name], [Value], [FieldId], [MasterId], [ParentId] FROM [GetItemView] WHERE [ItemId]= {2}itemId{3}";
      using (var reader = Api.CreateReader(GetItemFromViewUnsorted, parameters))
      {
        while (reader.Read())
        {
          // A vast majority of query results are expected to be item fields
          Tuple<string, int, ID, string> fld;
          if (TryReadItemField(reader, out fld))
          {
            // read field
            fields = fields ?? new List<Tuple<string, int, ID, string>>();

            fields.Add(fld);

            continue;
          }

          if ((itemDefPrefetchModel == null) && TryParseItemDefinition(itemId, reader, ref itemDefPrefetchModel))
          {
            continue;
          }

          ID childId;
          if (TryReadChildID(reader, out childId))
          {
            children = children ?? new List<ID>();

            children.Add(childId);

            continue;
          }

          Log.Error("Problems in reading {0} item. Please check SQL Data for integrity".FormatWith(itemId.ToString()), this);
        }
      }

      // Did not read item model, thus whole item was not read. Should not return anything
      if (itemDefPrefetchModel == null)
      {
        return;
      }

      /*
       *  Prefetch data always does not contain itemId in current implementation.
       *  Added just for safety
       */
      if (!prefetchData.ContainsKey(itemId))
      {
        prefetchData.Add(itemId, itemDefPrefetchModel);
      }

      children?.ForEach(t => itemDefPrefetchModel.AddChildId(t));

      if (fields == null)
      {
        return;
      }

      var languages = GetLanguages();

      var orderedFields = fields.OrderByDescending(t => t.Item1).ThenByDescending(t => t.Item2).ToArray();
      
      /*   IMPORTANT: 
           * Due to faulting Sitecore AddField method API.
           * MUST add Versioned fields FIRST ( to force creation of FieldList for needed language.ToUpper()¤VersionNumber )
           * Then MUST add unversioned ( check all item fieldLists where key starts with language.ToUpper() )
           * Shared Fields MUST GO LAST. See 'PrefetchData.AddSharedField' implementation for more details. ( Looping through all the fieldLists )
       */

      // Create field lists for all the languages  (Key => language.ToUpper()¤1 )
      itemDefPrefetchModel.InitializeFieldLists(languages);

      foreach (var t in orderedFields)
      {
        itemDefPrefetchModel.AddField
        (
          language: t.Item1,
          version: t.Item2,
          fieldId: t.Item3,
          value: t.Item4
        );
      }
    }

    #region Parsing Reader logic - transforming read rows into ItemDefinition

    /// <summary>
    /// Tries the read item field from current reader row.
    /// <para>Return true if field is read; false otherwise.</para>
    /// </summary>
    /// <param name="reader">The reader.</param>
    /// <param name="fld">Tuple of language, version, fldId, FldValue.</param>
    /// <returns><value>False</value> in case current reader row is not Field. Fld would be null as well; True if field was read from reader.</returns>
    private bool TryReadItemField([NotNull]DataProviderReader reader, out Tuple<string, int, ID, string> fld)
    {

      Assert.ArgumentNotNull(reader, "reader");
      /*
       *                             READER Columns
       *    0         1           2           3        4          5             6
       * Version | Language |  ItemName*  | Value | FieldID |  MasterID*  |  ParentID*  |
       *                    | MUST BE NULL|                 | MUST BE NULL| MUST BE NULL|
       * 
       * In case of field row, ItemName and MasterID would be null. Thus checking for null would be enough
       * 
       */

      var name = reader.InnerReader[2];
      var parentId = reader.InnerReader[6];
      if ((name is DBNull) && (parentId is DBNull))
      {
        var fldId = /*IDRepository.Intern*/Api.GetId(4, reader);
        var version = Api.GetInt(0, reader);

        var rawLangRead = (Api.GetString(1, reader));

        var language = string.IsNullOrEmpty(rawLangRead) ? rawLangRead : OptimizeLanguageString(rawLangRead);

        var value = OptimizeFieldValueString(Api.GetString(3, reader), fldId);
        fld = new Tuple<string, int, ID, string>(language, version, fldId, value);

        return true;
      }

      fld = null;
      return false;
    }



    private bool TryReadChildID(DataProviderReader reader, out ID childId)
    {
      /*
       *                             READER Columns
       *    0         1           2           3        4          5             6
       * Version* | Language* |  ItemName*  | Value* | FieldID* |  MasterID*  |  ChildID  |
       * 
       * NOTES: Since sort is done on Sitecore side, we must identify row type ( itemDefinition, child, field) by some condition.
       *        Only ParentID would be set in case child row.
       *        FieldID would be empty only in case of child row read.
       */
      childId = null;

      var fieldIdColumn = reader.InnerReader[4];

      var childIdColumn = reader.InnerReader[6];

      if (childIdColumn is DBNull)
      {
        return false;
      }

      if (fieldIdColumn is DBNull)
      {
        childId = /*IDRepository.Intern*/Api.GetId(6, reader);

        return true;
      }

      return false;
    }

    private bool TryParseItemDefinition(ID itemId, DataProviderReader reader, ref PrefetchData itemDefPrefetchModel)
    {

      /*
       *                             READER Columns
       *    0         1           2           3        4          5             6
       * Version* | Language* |  ItemName  | Value* | TemplateID |  MasterID  |  ParentID  |
       * 
       * NOTES: Since sort is done on Sitecore side, we must identify row type ( itemDefinition, child, field) by some condition.
       *        ItemName would only be set in case of Item row. 
       *        For children, fields ItemName is null. Thus check for 'ItemName is Null' can identify item row.
       * 
       */

      if ((reader.InnerReader[2] == null) || (reader.InnerReader[2] is DBNull))
      {
        return false;
      }

      var name = Api.GetString(2, reader);
      if (string.IsNullOrEmpty(name))
      {
        return false;
      }

      itemDefPrefetchModel = ParseItemDefinition(name, itemId, reader);

      return true;
    }

    [NotNull]
    private PrefetchData ParseItemDefinition([NotNull]string itemName, [NotNull]ID id, [NotNull] DataProviderReader reader)
    {
      var templateId = /*IDRepository.Intern*/(Api.GetId(4, reader));
      var branchId   = /*IDRepository.Intern*/(Api.GetId(5, reader));
      var parentId   = /*IDRepository.Intern*/(Api.GetId(6, reader));

      return new PrefetchData/*Interned*/(new ItemDefinition(id, itemName, templateId, branchId), parentId);
    }

    #endregion
                          
    #region To improve zone

    /// <summary>
    /// Lang string is met really often. Thus makes sence to intern it.    
    /// </summary>
    /// <param name="getString"></param>
    /// <returns></returns>
    protected virtual string OptimizeLanguageString(string getString)
    {
      // OOB interning is slowwww... Think about better nolocking approaches.
      /*
       * TODO: implement sort of interning here
       */

      return getString;
    }

    protected virtual string OptimizeFieldValueString(string fldValue, [NotNull] ID fldId)
    {
      // TODO: think about some fields like workflowID, WorkflowState, CreatedBy, UpdatedBy...

      return fldValue;
    }

    #endregion                                          

  }
}