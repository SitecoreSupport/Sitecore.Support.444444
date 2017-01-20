﻿namespace Sitecore.Abstract.Data.SqlServer
{
  using System;
  using System.Collections.Generic;
  using System.Threading;
  using Sitecore.Support.Reflection;
  using Sitecore.Caching;
  using Sitecore.Collections;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Data.DataProviders.Sql.FastQuery;
  using Sitecore.Data.Eventing;
  using Sitecore.Data.Items;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;
  using Sitecore.Reflection;

  [UsedImplicitly]
  public class SqlServerDataProvider : Sitecore.Data.SqlServer.SqlServerDataProvider
  {
    [CanBeNull]
    protected readonly Type EventQueueType;

    [CanBeNull]
    protected readonly PublishQueueProvider PublishQueueProvider;

    [CanBeNull]
    protected readonly DatabasePropertiesProvider DatabasePropertiesProvider;

    [NotNull]
    protected readonly Action<ItemDefinition, ItemChanges> UpdateItemDefinitionDelegate;

    [NotNull]
    protected readonly Action<ID, ItemChanges> UpdateItemFieldsDelegate;

    [NotNull]
    protected readonly Action<ID, ID> OnItemSavedDelegate;

    #region Constructors

    public SqlServerDataProvider([NotNull] string connectionString) : base(connectionString)
    {
      Assert.ArgumentNotNull(connectionString, nameof(connectionString));

      // Create Providers
      EventQueueType = null;
      PublishQueueProvider = null;
      DatabasePropertiesProvider = null;

      // Create Delegates
      UpdateItemDefinitionDelegate = CreateUpdateItemDefinitionDelegate();
      UpdateItemFieldsDelegate = CreateUpdateItemFieldsDelegate();
      OnItemSavedDelegate = CreateOnItemSavedDelegate();
    }

    public SqlServerDataProvider([NotNull] string connectionString, [NotNull] string eventQueueType, [NotNull] string publishQueueProviderType, [NotNull] string databasePropertiesProviderType) : base(connectionString)
    {
      Assert.ArgumentNotNull(connectionString, nameof(connectionString));
      Assert.ArgumentNotNull(eventQueueType, nameof(eventQueueType));
      Assert.ArgumentNotNull(publishQueueProviderType, nameof(publishQueueProviderType));
      Assert.ArgumentNotNull(databasePropertiesProviderType, nameof(databasePropertiesProviderType));

      // Create Providers
      EventQueueType = ParseEventQueue(eventQueueType);
      PublishQueueProvider = CreatePublishQueueProvider(publishQueueProviderType);
      DatabasePropertiesProvider = CreateDatabasePropertiesProvider(databasePropertiesProviderType);

      // Create Delegates
      UpdateItemDefinitionDelegate = CreateUpdateItemDefinitionDelegate();
      UpdateItemFieldsDelegate = CreateUpdateItemFieldsDelegate();
      OnItemSavedDelegate = CreateOnItemSavedDelegate();
    }

    #endregion

    #region Publication Properties

    [CanBeNull]
    [UsedImplicitly]
    public new SqlDataApi Api
    {
      get
      {
        return base.Api;
      }
    }

    [UsedImplicitly]
    public new TimeSpan CommandTimeout
    {
      get
      {
        return base.CommandTimeout;
      }
    }

    [UsedImplicitly]
    public new bool DescendantsShouldBeUpdated
    {
      get
      {
        return base.DescendantsShouldBeUpdated;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new string Disable
    {
      set
      {
        base.Disable = value;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new string DisableGroup
    {
      set
      {
        base.DisableGroup = value;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new string Enable
    {
      set
      {
        base.Enable = value;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new string EnableGroup
    {
      set
      {
        base.EnableGroup = value;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new LanguageCollection Languages
    {
      get
      {
        return base.Languages;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new Cache PrefetchCache
    {
      get
      {
        return base.PrefetchCache;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new Cache PropertyCache
    {
      get
      {
        return base.PropertyCache;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new Thread RebuildThread
    {
      get
      {
        return base.RebuildThread;
      }
    }

    [UsedImplicitly]
    public new bool SkipDescendantsUpdate 
    {
      get
      {
        return base.SkipDescendantsUpdate;
      }
    }

    [CanBeNull]
    [UsedImplicitly]
    public new QueryToSqlTranslator Translator
    {
      get
      {
        return base.Translator;
      }
    }

    #endregion

    #region Publication Methods

    [UsedImplicitly]
    public void UpdateItemDefinition([NotNull] ItemDefinition item, [NotNull] ItemChanges changes)
    {
      Assert.ArgumentNotNull(item, nameof(item));
      Assert.ArgumentNotNull(changes, nameof(changes));

      UpdateItemDefinitionDelegate(item, changes);
    }

    [UsedImplicitly]
    public void UpdateItemFields([NotNull] ID itemId, [NotNull] ItemChanges changes)
    {
      Assert.ArgumentNotNull(itemId, nameof(itemId));
      Assert.ArgumentNotNull(changes, nameof(changes));

      UpdateItemFieldsDelegate(itemId, changes);
    }

    [UsedImplicitly]
    public void OnItemSaved([NotNull] ID itemId, [NotNull] ID templateId)
    {
      Assert.ArgumentNotNull(itemId, nameof(itemId));
      Assert.ArgumentNotNull(templateId, nameof(templateId));


      OnItemSavedDelegate(itemId, templateId);
    }

    #endregion

    #region EventQueue

    [CanBeNull]
    public override EventQueue GetEventQueue()
    {
      var eventQueueType = EventQueueType;
      if (eventQueueType != null)
      {
        var eventQueue = CreateEventQueue(eventQueueType);
        if (eventQueue != null)
        {
          return eventQueue;
        }
      }

      return base.GetEventQueue();
    }

    #endregion

    #region PublishQueueProvider

    [CanBeNull]
    public override IDList GetPublishQueue(DateTime from, DateTime to, [NotNull] CallContext context)
    {
      var provider = PublishQueueProvider;
      if (provider == null)
      {
        return base.GetPublishQueue(from, to, context);
      }

      Assert.ArgumentNotNull(context, nameof(context));

      return provider.GetPublishQueue(from, to, context, this) as IDList ?? base.GetPublishQueue(from, to, context);
    }

    public override bool AddToPublishQueue([NotNull] ID itemID, [NotNull] string action, DateTime date, [NotNull] CallContext context)
    {
      var provider = PublishQueueProvider;
      if (provider == null)
      {
        return base.AddToPublishQueue(itemID, action, date, context);
      }

      Assert.ArgumentNotNull(itemID, nameof(itemID));
      Assert.ArgumentNotNull(action, nameof(action));
      Assert.ArgumentNotNull(context, nameof(context));

      return provider.AddToPublishQueue(itemID, action, date, context) || base.AddToPublishQueue(itemID, action, date, context);
    }

    #endregion

    #region DatabasePropertiesProvider

    [CanBeNull]
    public override string GetProperty([NotNull] string propertyName, [NotNull] CallContext context)
    {
      var provider = DatabasePropertiesProvider;
      if (provider == null)
      {
        return base.GetProperty(propertyName, context);
      }

      Assert.ArgumentNotNull(propertyName, nameof(propertyName));
      Assert.ArgumentNotNull(context, nameof(context));

      return provider.GetProperty(propertyName, context);
    }

    public override bool SetProperty([NotNull] string parameterName, [NotNull] string value, [NotNull] CallContext context)
    {
      var provider = DatabasePropertiesProvider;
      if (provider == null)
      {
        return base.SetProperty(parameterName, value, context);
      }

      Assert.ArgumentNotNull(parameterName, nameof(parameterName));
      Assert.ArgumentNotNull(value, nameof(value));
      Assert.ArgumentNotNull(context, nameof(context));

      return provider.SetProperty(parameterName, value, context);
    }

    public override bool RemoveProperty([NotNull] string propertyName, bool isPrefix, [NotNull] CallContext context)
    {
      var provider = DatabasePropertiesProvider;
      if (provider == null)
      {
        return base.RemoveProperty(propertyName, isPrefix, context);
      }

      Assert.ArgumentNotNull(propertyName, nameof(propertyName));
      Assert.ArgumentNotNull(context, nameof(context));

      return provider.RemoveProperty(propertyName, isPrefix, context);
    }

    [CanBeNull]
    public override List<string> GetPropertyKeys([NotNull] string prefix, [NotNull] CallContext context)
    {
      var provider = DatabasePropertiesProvider;
      if (provider == null)
      {
        return base.GetPropertyKeys(prefix, context);
      }

      Assert.ArgumentNotNull(prefix, nameof(prefix));
      Assert.ArgumentNotNull(context, nameof(context));

      return provider.GetPropertyKeys(prefix, context);
    }

    [CanBeNull]
    public string GetPropertyBase([CanBeNull] string propertyName, [CanBeNull] CallContext context)
    {
      return base.GetProperty(propertyName, context);
    }

    public bool SetPropertyBase([CanBeNull] string parameterName, [CanBeNull] string value, [CanBeNull] CallContext context)
    {
      return base.SetProperty(parameterName, value, context);
    }

    public bool RemovePropertyBase([CanBeNull] string propertyName, bool isPrefix, [CanBeNull] CallContext context)
    {
      return base.RemoveProperty(propertyName, isPrefix, context);
    }

    [CanBeNull]
    public List<string> GetPropertyKeysBase([CanBeNull] string prefix, [CanBeNull] CallContext context)
    {
      return base.GetPropertyKeys(prefix, context);
    }

    [CanBeNull]
    public string GetPropertyCoreBase([CanBeNull] string propertyName, [CanBeNull] CallContext context)
    {
      return base.GetPropertyCore(propertyName, context);
    }

    public bool SetPropertyCoreBase([CanBeNull] string parameterName, [CanBeNull] string value, [CanBeNull] CallContext context)
    {
      return base.SetPropertyCore(parameterName, value, context);
    }

    public bool RemovePropertyCoreBase([CanBeNull] string propertyName, bool isPrefix, [CanBeNull] CallContext context)
    {
      return base.RemovePropertyCore(propertyName, isPrefix, context);
    }

    [CanBeNull]
    protected override string GetPropertyCore([NotNull] string propertyName, [NotNull] CallContext context)
    {
      var provider = DatabasePropertiesProvider;
      if (provider == null)
      {
        return base.GetPropertyCore(propertyName, context);
      }

      Assert.ArgumentNotNull(propertyName, nameof(propertyName));
      Assert.ArgumentNotNull(context, nameof(context));

      return provider.GetPropertyCore(propertyName, context);
    }

    protected override bool SetPropertyCore([NotNull] string parameterName, [NotNull] string value, [NotNull] CallContext context)
    {
      var provider = DatabasePropertiesProvider;
      if (provider == null)
      {
        return base.SetPropertyCore(parameterName, value, context);
      }

      Assert.ArgumentNotNull(parameterName, nameof(parameterName));
      Assert.ArgumentNotNull(value, nameof(value));
      Assert.ArgumentNotNull(context, nameof(context));

      return provider.SetPropertyCore(parameterName, value, context);
    }

    protected override bool RemovePropertyCore([NotNull] string propertyName, bool isPrefix, [NotNull] CallContext context)
    {
      var provider = DatabasePropertiesProvider;
      if (provider == null)
      {
        return base.RemovePropertyCore(propertyName, isPrefix, context);
      }

      Assert.ArgumentNotNull(propertyName, nameof(propertyName));
      Assert.ArgumentNotNull(context, nameof(context));

      return provider.RemovePropertyCore(propertyName, isPrefix, context);
    }

    #endregion

    #region Methods

    [CanBeNull]
    private Type ParseEventQueue([CanBeNull] string assemblyQualifiedName)
    {
      if (string.IsNullOrEmpty(assemblyQualifiedName))
      {
        return null;
      }

      var type = Type.GetType(assemblyQualifiedName);
      if (type == null)
      {
        Log.Error($"Cannot find the \"{assemblyQualifiedName}\" type that is supposed to be an EventQueue.", this);
      }

      return type;
    }

    [CanBeNull]
    private EventQueue CreateEventQueue([NotNull] Type type)
    {
      Assert.ArgumentNotNull(type, nameof(type));

      try
      {
        var parameters = new object[] { Api, Database };
        var eventQueue = ReflectionUtil.CreateObject(type, parameters);
        Assert.IsNotNull(eventQueue, "eventQueue");

        return (EventQueue)eventQueue;
      }
      catch (Exception ex)
      {
        Log.Error($"Cannot instantiate the \"{type.AssemblyQualifiedName}\" type which represents an EventQueue. The default one will be used instead: {typeof(SqlServerEventQueue).AssemblyQualifiedName}", ex, this);
        return null;
      }
    }

    [CanBeNull]
    private PublishQueueProvider CreatePublishQueueProvider([CanBeNull] string assemblyQualifiedName)
    {
      if (string.IsNullOrEmpty(assemblyQualifiedName))
      {
        return null;
      }

      var type = Type.GetType(assemblyQualifiedName);
      if (type == null)
      {
        Log.Error($"Cannot find the \"{assemblyQualifiedName}\" type that is supposed to be an IPublishQueueProvider. The default one will be used instead: {typeof(Sitecore.Data.SqlServer.SqlServerDataProvider).AssemblyQualifiedName}", this);
        return null;
      }

      var publishQueueProvider = ReflectionUtil.CreateObject(type);
      Assert.IsNotNull(publishQueueProvider, "publishQueueProvider");

      return (PublishQueueProvider)publishQueueProvider;
    }

    [CanBeNull]
    private DatabasePropertiesProvider CreateDatabasePropertiesProvider([CanBeNull] string assemblyQualifiedName)
    {
      if (string.IsNullOrEmpty(assemblyQualifiedName))
      {
        return null;
      }

      var type = Type.GetType(assemblyQualifiedName);
      if (type == null)
      {
        Log.Error($"Cannot find the \"{assemblyQualifiedName}\" type that is supposed to be an IDatabasePropertiesProvider. The default one will be used instead: {typeof(Sitecore.Data.SqlServer.SqlServerDataProvider).AssemblyQualifiedName}", this);
        return null;
      }

      var databasePropertiesProvider = Activator.CreateInstance(type, this);
      Assert.IsNotNull(databasePropertiesProvider, "databasePropertiesProvider");

      return (DatabasePropertiesProvider)databasePropertiesProvider;
    }

    [NotNull]
    private Action<ID, ItemChanges> CreateUpdateItemFieldsDelegate()
    {
      return Helper.CreateMethodDelegate<ID, ItemChanges>(typeof(SqlDataProvider), this, "UpdateItemFields", true, false, false);
    }

    [NotNull]
    private Action<ItemDefinition, ItemChanges> CreateUpdateItemDefinitionDelegate()
    {
      return Helper.CreateMethodDelegate<ItemDefinition, ItemChanges>(typeof(SqlDataProvider), this, "UpdateItemDefinition", true, false, false);
    }

    [NotNull]
    private Action<ID, ID> CreateOnItemSavedDelegate()
    {
      return Helper.CreateMethodDelegate<ID, ID>(typeof(SqlDataProvider), this, "OnItemSaved", true, false, false);
    }

    #endregion
  }
}
