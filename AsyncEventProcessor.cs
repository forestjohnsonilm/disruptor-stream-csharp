﻿using System;
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
        Task OnNext(T @event, long sequence, bool endOfBatch, CancellationToken cancellationToken);
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
            Task.Run(async () =>
            {
                try {
                    await _implementation.OnNext(@event, sequence, lastInBatch, _cancellationTokenSource.Token);
                    OnCompleted(sequence);
                } 
                catch (Exception ex)
                {
                    // Todo figure this out.
                    throw;
                }
            }).ConfigureAwait(false);
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
            var enumerator = completed.GetEnumerator();
            enumerator.MoveNext();
            long completedSequence = enumerator.Current;
            while (enumerator.MoveNext() && enumerator.Current == completedSequence + 1)
            {
                completedSequence = enumerator.Current;
            }

            completed = new SortedSet<long>(completed.Where(x => x > completedSequence));

            return completedSequence;
        }

        public override void Halt()
        {
            base.Halt();
            _cancellationTokenSource.Cancel();
        }
    }
}