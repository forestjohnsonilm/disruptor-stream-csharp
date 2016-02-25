using System;
using System.Linq;
using NUnit.Framework;
using Disruptor.Dsl;
using Disruptor;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Diagnostics;

namespace DisruptorTest
{
    [TestFixture]
    public class DisruptorExample
    {

        private List<string> _resultLog = new List<string>();

        private List<Tuple<int, string>> _ratings = new List<Tuple<int, string>>();


        [SetUp]
        public void SetUp ()
        {
            _resultLog = new List<string>();
        }

        [Test, Combinatorial]
        public async Task DemonstrateDisruptor(
                [Values(1, 2)] int jsonParallelism,
                [Values(1024)] int ringSize,
                [Values("yield", "sleep" )] string waitStrategyName,
                [Values("multi-low-contention", "single")] string claimStrategyName
            )
        {
            var listsPerRequest = 5;
            var numberOfTodoLists = 3000;
            var numberOfUpdates = 3000;
            var maxNumberOfItemsPerList = 4;

            var waitStrategies = new Dictionary<string, IWaitStrategy>()
            {
                { "busy", new BusySpinWaitStrategy() },
                { "block", new BlockingWaitStrategy() },
                { "yield", new YieldingWaitStrategy() },
                { "sleep", new SleepingWaitStrategy() },
            };
            var claimStrategies = new Dictionary<string, IClaimStrategy>()
            {
                { "single", new SingleThreadedClaimStrategy(ringSize) },
                { "multi", new MultiThreadedClaimStrategy(ringSize) },
                { "multi-low-contention", new MultiThreadedLowContentionClaimStrategy(ringSize) },
            };

            var disruptor = new Disruptor<EventType>(
                () => new EventType(),
                claimStrategies[claimStrategyName],
                waitStrategies[waitStrategyName],
                TaskScheduler.Default
            );

            var ringBuffer = disruptor.RingBuffer;

            var deserialize = GetDeserializers(jsonParallelism);
            var groupIntoRequests = GetRequestBuilders(listsPerRequest);

            disruptor.HandleEventsWith(deserialize)
                .Then(groupIntoRequests);

            // Since the Request Senders are EventProcessors(async) instead of EventHandlers(synchronous)
            // We have to manually create a sequence barrier and pass it in instead of just calling .Then() again.
            var barrierUntilRequestsAreGrouped = disruptor.After(groupIntoRequests).AsSequenceBarrier();
            var sendRequests = GetRequestSenders(ringBuffer, barrierUntilRequestsAreGrouped, RequestSenderMode.Callback);

            disruptor.HandleEventsWith(sendRequests);

            var writeLog = GetFinalLoggingEventHandler();

            disruptor.After(sendRequests)
                .Then(writeLog);

            var configuredRingBuffer = disruptor.Start();

            // There is a bug in the Disruptor code that prevents custom EventProcessors from running automatically
            // so we start them manually. If they were already started, and we try to start them again,
            // it would throw an exception here. 
            foreach(var requestSender in sendRequests)
            {
                AsyncExtensions.CreateNewLongRunningTask(() => requestSender.Run(), (ex) => Assert.Fail(ex.StackTrace));
            }

            var eventPublisher = new EventPublisher<EventType>(configuredRingBuffer);

            var messages = FakeDataGenerator.Generate(numberOfTodoLists, numberOfUpdates, maxNumberOfItemsPerList);

            //Console.WriteLine("");
            //Console.WriteLine("===========================");
            //Console.WriteLine("");

            System.GC.Collect();
            await Task.Delay(new TimeSpan(0,0,0,0,100));

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < messages.Length; i++)
            {
                // PublishEvent will block if there is no space avaliable on the ring buffer.
                eventPublisher.PublishEvent((@event, sequence) => {
                    @event.IncomingMessage = messages[i];
                    return @event;
                });
            }
            // Shutdown will block until the ring buffer is empty.
            disruptor.Shutdown();
            timer.Stop();

            // Uncomment this to show a concise version of the requests that would have been sent. 
            //Console.WriteLine(string.Join("\n", _resultLog));

            //Console.WriteLine("");
            //Console.WriteLine("===========================");
            //Console.WriteLine("");

            var elapsedSeconds = (float)timer.ElapsedMilliseconds / 1000;
            var ratePerSecond = (int)Math.Round((float)numberOfUpdates / elapsedSeconds);

            var strategy =   $"{nameof(jsonParallelism)}: {jsonParallelism}, "
                           + $"{nameof(ringSize)}: {ringSize}, "
                           + $"{nameof(waitStrategyName)}: {waitStrategyName}, "
                           + $"{nameof(claimStrategyName)}: {claimStrategyName}.";

            Console.WriteLine("Took: " + timer.ElapsedMilliseconds + " ms to process " + numberOfUpdates + " updates ");
            Console.WriteLine("at a rate of " + ratePerSecond + " per second ");
            Console.WriteLine("using strategy: " + strategy);

            _ratings.Add(new Tuple<int, string>(ratePerSecond, strategy));
        }
        
        [TearDown]
        public void TearDown()
        {
            var topRated =
                _ratings.OrderByDescending(x => x.Item1)
                .Take(10)
                .Select(x => "rate of " + x.Item1 + " with " + x.Item2);

            Console.WriteLine(string.Join("\n", topRated));
            
        }
     

        private ParallelEventHandler<EventType>[] GetDeserializers(int parallelism)
        {
            Action<EventType, long, bool> deserializeAction = (@event, sequence, isEndOfBatch) =>
            {
                // I wrote an optimized deserializer to test the performance difference, to see if JsonConvert is slow.
                // It is actually a tiny bit faster, but not really worth the effort in the end.
                //@event.IncomingMessage.Content = JsonConvert.DeserializeObject<IncomingMessageContent>(@event.IncomingMessage.ContentJson);

                @event.IncomingMessage.Content = new OptimizedDeserializer().Deserialize(@event.IncomingMessage.ContentJson);
                @event.IncomingMessage.ContentJson = null;
            };

            return ParallelEventHandler<EventType>.Group(parallelism, deserializeAction);
        }

        private RequestBuilder<EventType>[] GetRequestBuilders(int listsPerRequest)
        {
            Func<EventType, OutgoingRequest> getDeletedListsRequest =
                (@event) => new OutgoingRequest()
                {
                    Content = @event.IncomingMessage.Content.TodoLists
                        .Where(x => x.RequestType == RequestType.Delete)
                        .Select(x => new TodoList(x.Id))
                };

            var deleteTodoListRequestBuilder = new RequestBuilder<EventType>(
                getDeletedListsRequest,
                (@event, outgoingRequest) => @event.DeleteTodoListsRequest = outgoingRequest,
                listsPerRequest
            );

            Func<EventType, OutgoingRequest> getCreateOrUpdateListsRequest =
                (@event) => {
                    var content = @event.IncomingMessage.Content;
                    return new OutgoingRequest()
                    {
                        Content = from todoList in content.TodoLists.Where(x => x.RequestType == RequestType.CreateOrUpdate)
                                  join lineItem in content.LineItems.Where(x => x.RequestType == RequestType.CreateOrUpdate)
                                      on todoList.Id equals lineItem.TodoListId into lineItems
                                  select new TodoList(todoList.Id, todoList.Version, todoList.Title, todoList.Description, lineItems)
                    };
                };

            var createOrUpdateListsRequestBuilder = new RequestBuilder<EventType>(
                getCreateOrUpdateListsRequest,
                (@event, outgoingRequest) => @event.CreateOrUpdateTodoListRequest = outgoingRequest,
                listsPerRequest
            );

            Func<EventType, OutgoingRequest> getDeletedListItemsRequest =
                (@event) => {
                    var content = @event.IncomingMessage.Content;
                    return new OutgoingRequest()
                    {
                        Content = from todoList in content.TodoLists.Where(x => x.RequestType == RequestType.CreateOrUpdate)
                                  join lineItem in content.LineItems.Where(x => x.RequestType == RequestType.Delete)
                                      on todoList.Id equals lineItem.TodoListId into lineItems
                                  select new TodoList(todoList.Id, lineItems)
                    };
                };

            var deletedListItemsRequestBuilder = new RequestBuilder<EventType>(
                getDeletedListItemsRequest,
                (@event, outgoingRequest) => @event.DeleteTodoListsRequest = outgoingRequest,
                listsPerRequest
            );

            return new RequestBuilder<EventType>[] {
                deleteTodoListRequestBuilder,
                createOrUpdateListsRequestBuilder,
                deletedListItemsRequestBuilder
            };
        }


        private IEventProcessor[] GetRequestSenders (
            RingBuffer<EventType> ringBuffer, 
            ISequenceBarrier sequenceBarrier,
            RequestSenderMode mode)
        {
            var mockExternalService = new MockExternalService<OutgoingRequest, int>();
            AsyncExtensions.CreateNewLongRunningTask(
                () =>  mockExternalService.Run(), 
                (ex) => Assert.Fail(ex.StackTrace)
            );

            var createOrUpdateRequestExecutor = new RequestSender<EventType, OutgoingRequest>(
                (@event) => @event.CreateOrUpdateTodoListRequest, mockExternalService, mode, (ex) => Assert.Fail(ex.StackTrace)
            );
            var deleteRequestExecutor = new RequestSender<EventType, OutgoingRequest>(
                (@event) => @event.DeleteTodoListsRequest, mockExternalService, mode, (ex) => Assert.Fail(ex.StackTrace)
            );
            var removeItemsRequestExecutor = new RequestSender<EventType, OutgoingRequest>(
                (@event) => @event.RemoveLineItemsRequest, mockExternalService, mode, (ex) => Assert.Fail(ex.StackTrace)
            );

            return new IEventProcessor[] {
                new AsyncEventProcessor<EventType>(
                    ringBuffer, sequenceBarrier, new SpinLock(), createOrUpdateRequestExecutor
                ),
                new AsyncEventProcessor<EventType>(
                    ringBuffer, sequenceBarrier, new SpinLock(),  deleteRequestExecutor
                ),
                new AsyncEventProcessor<EventType>(
                    ringBuffer, sequenceBarrier, new SpinLock(),  removeItemsRequestExecutor
                ),
            };
        }

        private IEventHandler<EventType> GetFinalLoggingEventHandler()
        {
            return new SimpleEventHandler<EventType>((@event, sequence, isEndOfBatch) =>
            {
                @event.IncomingMessage = null;
                if(@event.RemoveLineItemsRequest != null)
                {
                    LogRequest("RemoveLineItemsRequest", @event.RemoveLineItemsRequest);
                    @event.RemoveLineItemsRequest = null;
                }
                if (@event.CreateOrUpdateTodoListRequest != null)
                {
                    LogRequest("CreateOrUpdateTodoListRequest", @event.CreateOrUpdateTodoListRequest);
                    @event.CreateOrUpdateTodoListRequest = null;
                }
                if (@event.DeleteTodoListsRequest != null)
                {
                    LogRequest("DeleteTodoListsRequest", @event.DeleteTodoListsRequest);
                    @event.DeleteTodoListsRequest = null;
                }
            });
        }

        private void LogRequest(string prefix, OutgoingRequest request)
        {
            var idsAndVersions = String.Join(",\n", request.Content.Select(list => "    " + list.Id + "  Version: " + list.Version));
            _resultLog.Add(prefix + ":\n" + idsAndVersions);
        }


        public class EventType
        {
            public IncomingMessage IncomingMessage { get; set; }

            public OutgoingRequest DeleteTodoListsRequest { get; set; }
            public OutgoingRequest RemoveLineItemsRequest { get; set; }
            public OutgoingRequest CreateOrUpdateTodoListRequest { get; set; }
        }

    }
}
