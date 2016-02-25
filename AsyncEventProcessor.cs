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
    public interface AsyncEventProcessorImplementation<T>
    {
        void OnNext(T @event, long sequence, bool endOfBatch, CancellationToken cancellationToken, Action<long> callback);
    }

    public sealed class AsyncEventProcessor<T> : AbstractEventProcessor<T> where T : class, new()
    {
        private readonly AsyncEventProcessorImplementation<T> _implementation;
        private readonly ILock _lock;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private SortedSet<long> completed = new SortedSet<long>();
        private long currentDownstreamBarrierSequence = -1L;

        public AsyncEventProcessor(
                RingBuffer<T> ringBuffer, 
                ISequenceBarrier sequenceBarrier, 
                ILock @lock,
                AsyncEventProcessorImplementation<T> implementation
            ) 
            : base(ringBuffer, sequenceBarrier)
        {
            _implementation = implementation;
            _lock = @lock;
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public override void OnNextAvaliable (T @event, long sequence, bool lastInBatch)
        {
            _implementation.OnNext(@event, sequence, lastInBatch, _cancellationTokenSource.Token, (s) => OnCompleted(s));
        }

        public override void OnCompleted(long sequence)
        {
            _lock.WithLock(() => {
                completed.Add(sequence);
                long newDownstreamBarrierSequence = ConsumeContiguousCompletedSequence();
                if(newDownstreamBarrierSequence > currentDownstreamBarrierSequence)
                {
                    currentDownstreamBarrierSequence = newDownstreamBarrierSequence;
                    base.OnCompleted(newDownstreamBarrierSequence);
                }
            });
        }

        private long ConsumeContiguousCompletedSequence ()
        {
            using (var enumerator = completed.GetEnumerator())
            {
                long completedSequence = currentDownstreamBarrierSequence;
                while (enumerator.MoveNext() && enumerator.Current == completedSequence + 1)
                {
                    completedSequence = enumerator.Current;
                }

                completed = new SortedSet<long>(completed.Where(x => x > completedSequence));

                return completedSequence;
            }
        }
                

        public override void Halt()
        {
            base.Halt();
            _cancellationTokenSource.Cancel();
        }
    }
}
