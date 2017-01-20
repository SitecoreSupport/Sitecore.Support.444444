namespace Sitecore
{
  using System;
  using System.Collections.Generic;
  using System.Data.SqlClient;
  using System.Linq;
  using System.Reflection;
  using System.Threading;
  using Sitecore.Configuration;
  using Sitecore.Diagnostics;
  using Sitecore.StringExtensions;

  public static class AdvancedSettings
  {
    [NotNull]
    public static readonly string SystemDatabase = Settings.GetSetting("AdvancedSettings.SystemDatabase", "core");

    [NotNull]
    private static readonly ReaderWriterLockSlim Lock = new ReaderWriterLockSlim();

    [NotNull]
    private static readonly Dictionary<string, AdvancedSettingBase> Cache = new Dictionary<string, AdvancedSettingBase>();

    [NotNull, UsedImplicitly]
    public static AdvancedSetting<string> Create([NotNull] string name, [NotNull] string defaultValue)
    {
      Assert.ArgumentNotNull(name, "name");
      Assert.ArgumentNotNull(defaultValue, "defaultValue");

      return new AdvancedSetting<string>(name, defaultValue, x => x);
    }

    [NotNull, UsedImplicitly]
    public static AdvancedSetting<bool> Create([NotNull] string name, bool defaultValue)
    {
      Assert.ArgumentNotNull(name, "name");

      return new AdvancedSetting<bool>(name, defaultValue, bool.Parse);
    }

    [NotNull, UsedImplicitly]
    public static AdvancedSetting<int> Create([NotNull] string name, int defaultValue)
    {
      Assert.ArgumentNotNull(name, "name");

      return new AdvancedSetting<int>(name, defaultValue, int.Parse);
    }

    [NotNull, UsedImplicitly]
    public static AdvancedSetting<long> Create([NotNull] string name, long defaultValue)
    {
      Assert.ArgumentNotNull(name, "name");

      return new AdvancedSetting<long>(name, defaultValue, long.Parse);
    }

    [NotNull, UsedImplicitly]
    public static AdvancedSetting<double> Create([NotNull] string name, double defaultValue)
    {
      Assert.ArgumentNotNull(name, "name");

      return new AdvancedSetting<double>(name, defaultValue, double.Parse);
    }

    [NotNull, UsedImplicitly]
    public static AdvancedSetting<TimeSpan> Create([NotNull] string name, TimeSpan defaultValue)
    {
      Assert.ArgumentNotNull(name, "name");

      return new AdvancedSetting<TimeSpan>(name, defaultValue, TimeSpan.Parse);
    }

    internal static void Register<T>([NotNull] AdvancedSetting<T> advancedSetting)
    {
      Assert.ArgumentNotNull(advancedSetting, "advancedSetting");

      Lock.EnterWriteLock();
      try
      {
        Cache[advancedSetting.Name] = advancedSetting;
      }
      finally
      {
        Lock.ExitWriteLock();
      }
    }

    internal static void Poll([CanBeNull] object obj, [CanBeNull] EventArgs eventArgs)
    {
      lock (Cache)
      {
        try
        {
          using (var connection = OpenConnection(SystemDatabase))
          {
            if (connection == null)
            {
              return;
            }

            using (var sqlCommand = new SqlCommand("SELECT TOP 1 [Value] FROM [Properties] WITH (NOLOCK) WHERE [Key] = 'sc_settings'", connection))
            {
              var settingsText = sqlCommand.ExecuteScalar() as string;
              if (string.IsNullOrEmpty(settingsText))
              {
                AdvancedSettingBase[] settings;
                Lock.EnterReadLock();
                try
                {
                  settings = Cache.Values.ToArray();
                }
                finally
                {
                  Lock.ExitReadLock();
                }

                foreach (var setting in settings)
                {
                  if (setting != null && setting.RawValue != null)
                  {
                    setting.RawValue = null;
                  }
                }

                return;
              }

              var settingsArray = settingsText.Split(';');
              foreach (var keyValueText in settingsArray)
              {
                var keyValueArr = keyValueText.Split('=');
                if (keyValueArr.Length < 2)
                {
                  Log.Error("Wrong sc_settings value: " + keyValueText, typeof(AdvancedSettings));

                  continue;
                }

                var key = keyValueArr[0];
                var value = keyValueText.Substring(key.Length + 1);
                key = key.Trim();
                value = value.Trim();

                Lock.EnterReadLock();
                try
                {
                  AdvancedSettingBase setting;
                  if (Cache.TryGetValue(key, out setting) && setting != null && setting.RawValue != value)
                  {
                    setting.RawValue = value;
                  }
                }
                finally
                {
                  Lock.ExitReadLock();
                }
              }
            }
          }
        }
        catch (Exception ex)
        {
          Log.Warn("Error during polling core database by advanced settings".FormatWith(SystemDatabase), ex, typeof(AdvancedSettings));
        }
      }
    }

    [CanBeNull]
    private static SqlConnection OpenConnection([CanBeNull] string connectionStringName)
    {
      Assert.ArgumentNotNullOrEmpty(connectionStringName, "connectionStringName");

      if (String.IsNullOrEmpty(connectionStringName))
      {
        return null;
      }

      var connectionString = Settings.GetConnectionString(connectionStringName);
      if (connectionString == null)
      {
        return null;
      }

      try
      {
        var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
        Assert.IsNotNull(connectionStringBuilder, "connectionStringBuilder");
      }
      catch
      {
        // is not a SQL connection string
        return null;
      }

      var connection = new SqlConnection(connectionString);
      connection.Open();

      return connection;
    }

    [NotNull]
    public static IEnumerable<AdvancedSettingBase> GetAll()
    {
      Lock.EnterReadLock();
      try
      {
        return Cache.Values;
      }
      finally
      {
        Lock.ExitReadLock();
      }
    }
  }
}