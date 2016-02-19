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
                Func<T, Func<TAggregate, TAggregate>> getProcess,
                Func<TAggregate, Task> saveAggregate
            )
        {
            _lock = @lock;
            _idGetter = idGetter;
            _loadAggregate = getAggregate;
            _getProcess = getProcess;
            _saveAggregate = saveAggregate;
        }

        private delegate TAggregate Process(T @event, TAggregate aggregate);

        private readonly Dictionary<Guid, Queue<Func<TAggregate, TAggregate>>> _blocked = new Dictionary<Guid, Queue<Func<TAggregate, TAggregate>>>();
        private readonly ILock _lock;
        private readonly Func<T, Guid> _idGetter;
        
        private readonly Func<T, Task<TAggregate>> _loadAggregate;
        private readonly Func<T, Func<TAggregate, TAggregate>> _getProcess;
        private readonly Func<TAggregate, Task> _saveAggregate;

        public async Task OnNext(T @event, long sequence, bool endOfBatch, CancellationToken cancellationToken)
        {
            var isFirstTaskForAggregate = EnqueueAndReturnTrueIfFirst(@event);

            if (isFirstTaskForAggregate)
            {
                await DrainQueueForAggregate(@event);
            }
        }

        private bool EnqueueAndReturnTrueIfFirst(T @event)
        {
            var aggregateId = _idGetter(@event);
            bool isFirstTaskInQueue = false;
            _lock.WithLock(() => {
                Queue<Func<TAggregate, TAggregate>> queueForAggregate;
                if (!_blocked.TryGetValue(aggregateId, out queueForAggregate))
                {
                    queueForAggregate = new Queue<Func<TAggregate, TAggregate>>();
                    _blocked.Add(aggregateId, queueForAggregate);
                }
                queueForAggregate.Enqueue(_getProcess(@event));

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
                    Func<TAggregate, TAggregate> nextProcessInQueue = _blocked[aggregateId].Dequeue();
                    aggregate = nextProcessInQueue(aggregate);
                }
            });

            await _saveAggregate(aggregate);
        }
    }
}
