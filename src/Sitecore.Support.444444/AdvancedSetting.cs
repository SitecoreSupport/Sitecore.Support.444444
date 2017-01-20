namespace Sitecore
{
  using System;
  using Sitecore.Configuration;
  using Sitecore.Diagnostics;

  public class AdvancedSetting<T> : AdvancedSettingBase
  {
    private readonly Func<string, T> Parse;

    private readonly T DefaultValue;

    private bool cached;

    private T cachedValue;

    public AdvancedSetting([NotNull] string name, T defaultValue, [NotNull] Func<string, T> parse)
      : base(name)
    {
      Assert.ArgumentNotNull(name, "name");
      Assert.ArgumentNotNull(parse, "parse");

      this.DefaultValue = defaultValue;
      this.Parse = parse;

      AdvancedSettings.Register(this);
    }

    public T Value
    {
      get
      {
        if (this.cached)
        {
          return this.cachedValue;
        }

        var rawValue = this.RawValue ?? Settings.GetSetting(this.Name);
        var value = rawValue != null ? this.Parse(rawValue) : this.DefaultValue;

        this.cachedValue = value;        
        this.cached = true;

        return value;
      }
    }

    internal override string RawValue
    {
      get
      {
        return base.RawValue;
      }

      set
      {
        this.cached = false;

        base.RawValue = value;
      }
    }
  }
}