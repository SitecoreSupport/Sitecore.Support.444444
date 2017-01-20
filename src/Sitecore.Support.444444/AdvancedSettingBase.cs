namespace Sitecore
{
  using System;
  using System.Collections.Generic;
  using Sitecore.Diagnostics;

  public abstract class AdvancedSettingBase
  {
    public readonly string Name;

    private static readonly EventArgs EmptyArgs = new EventArgs();

    [NotNull]
    private readonly List<EventHandler> Subscribers = new List<EventHandler>();

    [CanBeNull]
    private string rawValue;

    protected AdvancedSettingBase([NotNull] string name)
    {
      Assert.ArgumentNotNull(name, "name");

      this.Name = name;
    }

    [UsedImplicitly]
    public event EventHandler ValueChanged
    {
      add
      {
        this.Subscribers.Add(value);
      }

      remove
      {
        this.Subscribers.Remove(value);
      }
    }

    [CanBeNull]
    internal virtual string RawValue
    {
      get
      {
        return this.rawValue;
      }

      set
      {
        Log.Info(string.Format("AdvancedSetting \"{0}\" value has been changed to \"{1}\"", this.Name, value ?? "<null>"), this);

        this.rawValue = value;

        foreach (var subscriber in this.Subscribers)
        {
          if (subscriber != null)
          {
            subscriber(this, EmptyArgs);
          }
        }
      }
    }
  }
}