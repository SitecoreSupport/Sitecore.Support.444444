namespace Sitecore.Abstract.Data.SqlServer
{
  using System;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders;
  using Sitecore.Diagnostics;

  public abstract class PublishQueueProvider
  {
    [CanBeNull]
    [UsedImplicitly]
    public virtual object GetPublishQueue(DateTime from, DateTime to, [NotNull] CallContext context, [NotNull] SqlServerDataProvider dataProvider)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(dataProvider, "dataProvider");

      return null;
    }

    [UsedImplicitly]
    public virtual bool AddToPublishQueue([NotNull] ID itemId, [NotNull] string action, DateTime date, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemId, "itemId");
      Assert.ArgumentNotNull(action, "action");
      Assert.ArgumentNotNull(context, "context");

      return false;
    }
  }
}