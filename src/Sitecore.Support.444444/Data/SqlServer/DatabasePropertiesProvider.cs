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
      Assert.ArgumentNotNull(dataProvider, "dataProvider");

      this.DataProvider = dataProvider;
    }

    [CanBeNull]
    public virtual string GetProperty([NotNull] string propertyName, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(propertyName, "propertyName");
      Assert.ArgumentNotNull(context, "context");

      return this.DataProvider.GetPropertyBase(propertyName, context);
    }

    [CanBeNull]
    public virtual string GetPropertyCore([NotNull] string propertyName, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(propertyName, "propertyName");
      Assert.ArgumentNotNull(context, "context");

      return this.DataProvider.GetPropertyCoreBase(propertyName, context);
    }

    public virtual bool SetProperty([NotNull] string parameterName, [NotNull] string value, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(parameterName, "parameterName");
      Assert.ArgumentNotNull(value, "value");
      Assert.ArgumentNotNull(context, "context");

      return this.DataProvider.SetPropertyBase(parameterName, value, context);
    }

    public virtual bool SetPropertyCore([NotNull] string parameterName, [NotNull] string value, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(parameterName, "parameterName");
      Assert.ArgumentNotNull(value, "value");
      Assert.ArgumentNotNull(context, "context");

      return this.DataProvider.SetPropertyCoreBase(parameterName, value, context);
    }

    public virtual bool RemoveProperty([NotNull] string propertyName, bool isPrefix, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(propertyName, "propertyName");
      Assert.ArgumentNotNull(context, "context");

      return this.DataProvider.RemovePropertyBase(propertyName, isPrefix, context);
    }

    public virtual bool RemovePropertyCore([NotNull] string propertyName, bool isPrefix, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(propertyName, "propertyName");
      Assert.ArgumentNotNull(context, "context");

      return this.DataProvider.RemovePropertyCoreBase(propertyName, isPrefix, context);
    }

    [CanBeNull]
    public virtual List<string> GetPropertyKeys([NotNull] string prefix, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(prefix, "prefix");
      Assert.ArgumentNotNull(context, "context");

      return this.DataProvider.GetPropertyKeysBase(prefix, context);
    }
  }
}