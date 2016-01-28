using System;
using NUnit.Framework;
using Disruptor.Dsl;
using Disruptor;
using System.Threading.Tasks;

namespace DisruptorTest
{
    [TestFixture]
    public class DisruptorExample
    {
        [Test]
        public void DemonstrateDisruptor()
        {
            var disruptor = new Disruptor<EventType>(() => new EventType(), (int)Math.Pow(2, 10), TaskScheduler.Default);

            //disruptor.HandleEventsWith
        }

        class MessageAuthenticator<T> : AsyncEventProcessor<T> where T : class, new()
        {
            public MessageAuthenticator(RingBuffer<T> ringBuffer, ISequenceBarrier sequenceBarrier, ILock @lock) 
            : base(ringBuffer, sequenceBarrier, @lock) { }

            public override void OnNextAvaliable(T @event, long sequence, bool lastInBatch)
            {
                Task.Run(async () =>
                {
                    await Task.Delay(200);
                    OnCompleted(sequence);
                });
            }
        }

        class EventType
        {
            public string Name = "a";
        }
    }
}
