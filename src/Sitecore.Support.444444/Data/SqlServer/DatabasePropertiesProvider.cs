namespace Sitecore.Abstract.Data.SqlServer
{
  using System.Collections.Generic;
  using Sitecore.Data.DataProviders;
  using Sitecore.Diagnostics;

  public abstract class DatabasePropertiesProvider
  {
    [NotNull]
    [UsedImplicitly]
    protected readonly SqlServerDataProvider DataProvider;

    protected DatabasePropertiesProvider([NotNull] SqlServerDataProvider dataProvider)
    {
      Assert.ArgumentNotNull(dataProvider, nameof(dataProvider));

      DataProvider = dataProvider;
    }

    [CanBeNull]
    public virtual string GetProperty([NotNull] string propertyName, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(propertyName, nameof(propertyName));
      Assert.ArgumentNotNull(context, nameof(context));

      return DataProvider.GetPropertyBase(propertyName, context);
    }

    [CanBeNull]
    public virtual string GetPropertyCore([NotNull] string propertyName, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(propertyName, nameof(propertyName));
      Assert.ArgumentNotNull(context, nameof(context));

      return DataProvider.GetPropertyCoreBase(propertyName, context);
    }

    public virtual bool SetProperty([NotNull] string parameterName, [NotNull] string value, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(parameterName, nameof(parameterName));
      Assert.ArgumentNotNull(value, nameof(value));
      Assert.ArgumentNotNull(context, nameof(context));

      return DataProvider.SetPropertyBase(parameterName, value, context);
    }

    public virtual bool SetPropertyCore([NotNull] string parameterName, [NotNull] string value, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(parameterName, nameof(parameterName));
      Assert.ArgumentNotNull(value, nameof(value));
      Assert.ArgumentNotNull(context, nameof(context));

      return DataProvider.SetPropertyCoreBase(parameterName, value, context);
    }

    public virtual bool RemoveProperty([NotNull] string propertyName, bool isPrefix, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(propertyName, nameof(propertyName));
      Assert.ArgumentNotNull(context, nameof(context));

      return DataProvider.RemovePropertyBase(propertyName, isPrefix, context);
    }

    public virtual bool RemovePropertyCore([NotNull] string propertyName, bool isPrefix, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(propertyName, nameof(propertyName));
      Assert.ArgumentNotNull(context, nameof(context));

      return DataProvider.RemovePropertyCoreBase(propertyName, isPrefix, context);
    }

    [CanBeNull]
    public virtual List<string> GetPropertyKeys([NotNull] string prefix, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(prefix, nameof(prefix));
      Assert.ArgumentNotNull(context, nameof(context));

      return DataProvider.GetPropertyKeysBase(prefix, context);
    }
  }
}