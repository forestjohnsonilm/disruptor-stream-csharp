using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Disruptor;
using Disruptor.Dsl;
using System.Threading;

namespace DisruptorTest
{
    public abstract class AbstractEventProcessor<T> : IEventProcessor where T : class, new()
    {
        private readonly RingBuffer<T> _ringBuffer;
        private readonly ISequenceBarrier _upstreamBarrier;
        private readonly Sequence _downstreamBarrier;
        
        private long currentUpstreamSequence = 0L;
        private volatile int runningState = (int)RunState.Halted;

        public AbstractEventProcessor(RingBuffer<T> ringBuffer, ISequenceBarrier sequenceBarrier)
        {
            _ringBuffer = ringBuffer;
            _upstreamBarrier = sequenceBarrier;
            _downstreamBarrier = new Sequence(-1L);
        }

        public virtual void Run()
        {
            CompareAndSwapRunningState(RunState.Running, RunState.Halted);
            while (runningState == (int)RunState.Running)
            {
                var avaliableUpstreamSequence = _upstreamBarrier.WaitFor(currentUpstreamSequence);
                while(currentUpstreamSequence <= avaliableUpstreamSequence)
                {
                    OnNextAvaliable(
                        _ringBuffer[currentUpstreamSequence], 
                        currentUpstreamSequence, 
                        currentUpstreamSequence < avaliableUpstreamSequence
                        );
                    currentUpstreamSequence++;
                }
                
            }
        }

        public abstract void OnNextAvaliable(T @event, long sequence, bool lastInBatch);

        public virtual void OnCompleted(long sequence)
        {
            _downstreamBarrier.LazySet(sequence);
        }

        public virtual void Halt()
        {
            CompareAndSwapRunningState(RunState.Halted, RunState.Running);
        }

        public Sequence Sequence => _downstreamBarrier;

        private void CompareAndSwapRunningState(RunState newState, RunState expectedState)
        {
            var originalValue = Interlocked.CompareExchange(ref runningState, (int)newState, (int)expectedState);
            if (originalValue != (int)expectedState)
            {
                throw new InvalidOperationException($"AbstractEventProcessor RunState was set to {newState} when already {(RunState)originalValue}.");
            }
        }

        private enum RunState
        {
            Halted = 0,
            Running = 1,
        }
    }
}
