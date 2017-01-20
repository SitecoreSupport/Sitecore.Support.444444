namespace Sitecore.Support.Data.ParallelEventQueue
{
  using System.Collections.Concurrent;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;

  internal sealed class ThreadStartParams
  {
    public readonly int ThreadNumber;

    [NotNull]
    public readonly ConcurrentQueue<EventHandlerPair> Queue;

    [NotNull]
    public readonly QueuedEvent[] ProcessedEvents;

    public ThreadStartParams([NotNull] ConcurrentQueue<EventHandlerPair> queue, int threadNumber, [NotNull] QueuedEvent[] processedEvents)
    {
      Assert.ArgumentNotNull(queue, "queue");
      Assert.ArgumentNotNull(processedEvents, "processedEvents");
      this.Queue = queue;
      this.ThreadNumber = threadNumber;
      this.ProcessedEvents = processedEvents;
    }
  }
}