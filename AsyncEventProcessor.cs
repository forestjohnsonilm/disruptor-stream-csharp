using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Disruptor;
using Disruptor.Dsl;
using System.Threading;

namespace DisruptorTest
{
    public abstract class AsyncEventProcessor<T> : AbstractEventProcessor<T> where T : class, new()
    {
        private readonly ConcurrentBag<long> _completed;
        private CancellationTokenSource cancellationTokenSource;

        public AsyncEventProcessor(RingBuffer<T> ringBuffer, ISequenceBarrier sequenceBarrier) : base(ringBuffer, sequenceBarrier)
        {
            _completed = new ConcurrentBag<long>();
        }

        public override void Run()
        {
            base.Run();
            cancellationTokenSource = new CancellationTokenSource();
        }


        public sealed override void OnCompleted(long sequence)
        {
            _completed.Add(sequence);

            _completed.

            base.OnCompleted();
            _downstreamBarrier.LazySet(sequence);
        }

        public void Halt()
        {
            base.Halt();
            cancellationTokenSource.Cancel();
        }
    }
}
