using System;
using System.Linq;
using NUnit.Framework;
using Disruptor.Dsl;
using Disruptor;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;

namespace DisruptorTest
{
    [TestFixture]
    public class DisruptorExample
    {
        [Test]
        public void DemonstrateDisruptor()
        {
            var disruptor = new Disruptor<EventType>(() => new EventType(), (int)Math.Pow(2, 10), TaskScheduler.Default);

            var ringBuffer = disruptor.RingBuffer;

            var eventPublishedBarrier = ringBuffer.NewBarrier(new Sequence[0]);

            var messageAuthenticator = new AsyncEventProcessor<EventType>(
                    disruptor.RingBuffer,
                    eventPublishedBarrier,
                    new SpinLock(),
                    new MessageAuthenticator()
                );

            var messageAuthenticatedBarrier = disruptor.HandleEventsWith(messageAuthenticator).AsSequenceBarrier();

            var writeAheadLogger = new AsyncEventProcessor<EventType>(
                    disruptor.RingBuffer,
                    eventPublishedBarrier,
                    new SpinLock(),
                    new WriteAheadLogger()
                );

            var writeAheadLoggerBarrier = disruptor.HandleEventsWith(writeAheadLogger).AsSequenceBarrier();

            var transactor = new AsyncEventProcessor<EventType>(
                    disruptor.RingBuffer,
                    writeAheadLoggerBarrier,
                    new SpinLock(),
                    new TransactionBoundaryBlocker<EventType>(
                            new SpinLock(),
                            (aggregate) => aggregate.AggregateId,
                            async (aggregate) =>
                            {
                                // TODO refactor this so that there are three functions: Get entity, Modify entity, and Save entity.
                                await Task.Delay(200);
                                return aggregate;
                            }
                        )
                );
        }

        public class MessageAuthenticator : AsyncEventProcessorImplementation<EventType> {
            public async Task OnNext(EventType @event, long sequence, bool endOfBatch, CancellationToken cancellationToken)
            {
                await Task.Delay(200, cancellationToken);
            }
        }

        public class WriteAheadLogger : AsyncEventProcessorImplementation<EventType>
        {
            public async Task OnNext(EventType @event, long sequence, bool endOfBatch, CancellationToken cancellationToken)
            {
                await Task.Delay(200, cancellationToken);
            }
        }

        

        public class EventType
        {
            public Guid AggregateId;
            public string Name = "a";
        }
    }
}
