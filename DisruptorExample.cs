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
        private readonly InMemoryDatabase _inMemoryDatabase = new InMemoryDatabase();

        [Test]
        public void DemonstrateDisruptor()
        {
            var disruptor = new Disruptor<EventType>(() => new EventType(), (int)Math.Pow(2, 10), TaskScheduler.Default);

            var ringBuffer = disruptor.RingBuffer;

            var eventPublishedBarrier = ringBuffer.NewBarrier(new Sequence[0]);

            var writeAheadLogger = new AsyncEventProcessor<EventType>(
                    disruptor.RingBuffer,
                    eventPublishedBarrier,
                    new SpinLock(),
                    new WriteAheadLogger()
                );

            var writeAheadLoggerBarrier = disruptor.HandleEventsWith(writeAheadLogger).AsSequenceBarrier();

            var exampleCommandProcessor = new AsyncEventProcessor<EventType>(
                    disruptor.RingBuffer,
                    writeAheadLoggerBarrier,
                    new SpinLock(),
                    new SimpleAggregateProcessor<EventType, ExampleAggregate>(
                            new SpinLock(),
                            (@event) => @event.ExampleAggregateId,
                            (@event) => _inMemoryDatabase.GetOrCreate(@event.ExampleAggregateId),
                            (@event) => {
                                return (aggregate) =>
                                {
                                    var result = @event.ExampleCommand.Process(aggregate);
                                    result.Version++;
                                    return result;
                                };
                            },
                            (aggregate) => _inMemoryDatabase.Upsert(aggregate)
                        )
                );
        }

        public class WriteAheadLogger : AsyncEventProcessorImplementation<EventType>
        {
            public async Task OnNext(EventType @event, long sequence, bool endOfBatch, CancellationToken cancellationToken)
            {
                // Simulate a delay reaching a logging service
                await Task.Delay(200, cancellationToken);
            }
        }

        

        public class EventType
        {
            public Guid ExampleAggregateId;
            public ICommand<ExampleAggregate> ExampleCommand;
        }

        public interface ICommand<T>
        {
            T Process(T input);
        }

        public class ExampleCountingCommand : ICommand<ExampleAggregate>
        {
            public int ToAdd { get; set; }

            public ExampleAggregate Process(ExampleAggregate input)
            {
                input.Sum += ToAdd;
                return input;
            }
        }

        public class ExampleAggregate : ICloneable
        {
            public Guid Id { get; set; }
            public int Sum { get; set; }
            public int Version { get; set; }

            public object Clone()
            {
                return this.MemberwiseClone();
            }
        }

        public class InMemoryDatabase
        {
            private readonly Dictionary<Guid, ExampleAggregate> _byId = new Dictionary<Guid, ExampleAggregate>();

            public async Task<ExampleAggregate> GetOrCreate(Guid id)
            {
                // Simulate a delay reaching a database over the network
                await Task.Delay(200);
                if (_byId.ContainsKey(id))
                {
                    return  (ExampleAggregate)_byId[id].Clone();
                }
                else
                {
                    var aggregate = new ExampleAggregate() { Id = id, Sum = 0 };
                    _byId[aggregate.Id] = aggregate;
                    return aggregate;
                }
            }

            public async Task Upsert(ExampleAggregate aggregate)
            {
                // Simulate a delay reaching a database over the network
                await Task.Delay(200);
                _byId[aggregate.Id] = aggregate;
            }
        }
    }
}
