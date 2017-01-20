namespace Sitecore.Support.Reflection
{
  using System;
  using Sitecore.Diagnostics;
  using Sitecore.Reflection;

  internal static class Helper
  {
    [NotNull]
    public static Action<T1, T2> CreateMethodDelegate<T1, T2>([NotNull] Type type, [NotNull] object obj, [NotNull] string name, bool includeNonPublic, bool includeInherited, bool includeStatic)
    {
      Assert.ArgumentNotNull(type, "type");
      Assert.ArgumentNotNull(obj, "obj");
      Assert.ArgumentNotNull(name, "name");

      var parameters = new[]
      {
        typeof(T1), typeof(T2)
      };
      var methodInfo = ReflectionUtil.GetMethod(type, name, includeNonPublic, includeInherited, includeStatic, parameters);
      Assert.IsNotNull(methodInfo, "methodInfo");

      var result = methodInfo.CreateDelegate(typeof(Action<T1, T2>), obj);
      Assert.IsNotNull(result, "result");

      return (Action<T1, T2>)result;
    }
  }
}