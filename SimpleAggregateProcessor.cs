using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Disruptor;
using Disruptor.Dsl;
using System.Threading;

namespace DisruptorTest
{
    public class SimpleAggregateProcessor<T, TAggregate> : AsyncEventProcessorImplementation<T>
    {
        public SimpleAggregateProcessor(
                ILock @lock, 
                Func<T, Guid> idGetter, 
                Func<T, Task<TAggregate>> getAggregate,
                Func<T, TAggregate, TAggregate> processAggregate,
                Func<TAggregate, Task> saveAggregate
            )
        {
            _lock = @lock;
            _idGetter = idGetter;
            _loadAggregate = getAggregate;
            _processAggregate = processAggregate;
            _saveAggregate = saveAggregate;
        }

        private readonly Dictionary<Guid, Queue<Action<T>>> _blocked = new Dictionary<Guid, Queue<Action<T>>>();
        private readonly ILock _lock;
        private readonly Func<T, Guid> _idGetter;
        
        private readonly Func<T, Task<TAggregate>> _loadAggregate;
        private readonly Func<T, TAggregate, TAggregate> _processAggregate;
        private readonly Func<T, Task> _saveAggregate;

        public async Task OnNext(T @event, long sequence, bool endOfBatch, CancellationToken cancellationToken)
        {
            var isFirstTaskForAggregate = EnqueueAndReturnTrueIfFirst(@event);

            if (isFirstTaskForAggregate)
            {
                await DrainQueueForAggregate(@event);
            }
        }

        private bool EnqueueAndReturnTrueIfFirst(T aggregate)
        {
            var aggregateId = _idGetter(aggregate);
            bool isFirstTaskInQueue = false;
            _lock.WithLock(() => {
                Queue<Action<T>> queueForAggregate;
                if (!_blocked.TryGetValue(aggregateId, out queueForAggregate))
                {
                    queueForAggregate = new Queue<Action<T>>();
                    _blocked.Add(aggregateId, queueForAggregate);
                }
                queueForAggregate.Enqueue();

                isFirstTaskInQueue = queueForAggregate.Count == 1;
            });

            return isFirstTaskInQueue;
        }

        private async Task DrainQueueForAggregate(T @event)
        {
            var aggregateId = _idGetter(@event);
            var queueForAggregate = _blocked[aggregateId];

            var aggregate = await _loadAggregate(@event);

            _lock.WithLock(() =>
            {
                while (queueForAggregate.Count > 0)
                {
                    Action<T> nextCommandInQueue = _blocked[aggregateId].Dequeue();
                    nextCommandInQueue(@event);
                }
            });

            await _saveAggregate(@event);
        }
    }
}
