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

            disruptor.HandleEventsWith
        }

        class DatabaseReader : AbstractEventProcessor<EventType>
        {


            public override void OnNextAvaliable(EventType @event, long sequence, bool lastInBatch)
            {
                
            }
        }

        class EventType
        {
            public string Name = 1;
        }
    }
}
