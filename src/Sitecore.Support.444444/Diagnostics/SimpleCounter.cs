namespace Sitecore.Support.Diagnostics
{
  public class SimpleCounter
  {
    public int Value { get; private set; }

    public void Reset()
    {
      Value = 0;
    }

    public void Increment()
    {
      Value++;
    }
  }
}