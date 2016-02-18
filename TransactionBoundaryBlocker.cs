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
    public class TransactionBoundaryBlocker<T> : AsyncEventProcessorImplementation<T>
    {
        public TransactionBoundaryBlocker(
                ILock @lock, 
                Func<T, Guid> idGetter, 
                Func<T, Task> getAggregate,
                Action<T> processAggregate,
                Func<T, Task> saveAggregate
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

        // TODO refactor this so that there are three functions: Get entity, Modify entity, and Save entity.
        private readonly Func<T, Task> _loadAggregate;
        private readonly Action<T> _processAggregate;
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
                queueForAggregate.Enqueue(_processAggregate);

                isFirstTaskInQueue = queueForAggregate.Count == 1;
            });

            return isFirstTaskInQueue;
        }

        private async Task DrainQueueForAggregate(T @event)
        {
            var aggregateId = _idGetter(@event);
            var queueForAggregate = _blocked[aggregateId];

            await _loadAggregate(@event);

            _lock.WithLock(() =>
            {

            });

            bool moreToConsume = true;
            while (moreToConsume == true)
            {
                Action<T> nextTaskInQueue = null;
                
                    if (queueForAggregate.Count == 0)
                    {
                        _blocked.Remove(aggregateId);
                        nextTaskInQueue = null;
                        moreToConsume = false;
                    }
                    else
                    {
                        nextTaskInQueue = _blocked[aggregateId].Dequeue();
                    }

                if (nextTaskInQueue != null)
                {
                    nextTaskInQueue(@event);
                }
            }

            await _saveAggregate(@event);
        }
    }
}
