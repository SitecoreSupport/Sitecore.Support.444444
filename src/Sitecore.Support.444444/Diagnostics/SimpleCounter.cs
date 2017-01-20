namespace Sitecore.Support.Diagnostics
{
  public class SimpleCounter
  {
    public int Value { get; private set; }

    public void Reset()
    {
      this.Value = 0;
    }

    public void Increment()
    {
      this.Value++;
    }
  }
}