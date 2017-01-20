namespace Sitecore.Support.Data.Eventing
{
  using System.Collections.Concurrent;
  using System.Linq;

  using Sitecore.Data;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Diagnostics;
  using Sitecore.StringExtensions;
  using Sitecore.Support.Data.ParallelEventQueue;

  [UsedImplicitly]
  public sealed class ParallelEventQueueWithSeparateQueues : ParallelSqlServerEventQueue
  {
    [NotNull]
    private readonly ConcurrentQueue<EventHandlerPair>[] queues;

    public ParallelEventQueueWithSeparateQueues([NotNull] SqlDataApi api, [NotNull] Database database)
      : base(api, database)
    {
      Assert.ArgumentNotNull(api, "api");
      Assert.ArgumentNotNull(database, "database");

      if (this.UseBaseFunctionality)
      {
        return;
      }

      // create queues for events
      this.queues = Enumerable
        .Range(0, ParallelEventQueueSettings.ParallelThreadsCount)
        .Select(x => new ConcurrentQueue<EventHandlerPair>())
        .ToArray();
      
      this.StartThreads();
    }
    
    protected override ConcurrentQueue<EventHandlerPair> GetQueue(int threadIndex)
    {
      var queue = this.queues[threadIndex];
      Assert.IsNotNull(queue, "The queue #{0} is null".FormatWith(threadIndex));

      return queue;
    }
  }
}
