namespace Sitecore.Support.Data.ParallelEventQueue
{
  using System;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;

  public struct EventHandlerPair
  {
    [NotNull]
    public readonly Action<object, Type> Handler;

    [NotNull]
    public readonly QueuedEvent QueuedEvent;

    public EventHandlerPair([NotNull] Action<object, Type> handler, [NotNull] QueuedEvent queuedEvent)
    {
      Assert.ArgumentNotNull(handler, nameof(handler));
      Assert.ArgumentNotNull(queuedEvent, nameof(queuedEvent));

      Handler = handler;
      QueuedEvent = queuedEvent;
    }
  }
}