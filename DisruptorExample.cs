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
        [Test]
        public async Task DemonstrateDisruptor()
        {
            var parallelism = 4;
            var listsPerRequest = 5;
            var numberOfTodoLists = 10;
            var numberOfUpdates = 10;
            var maxNumberOfItemsPerList = 4;

            var ringSize = (int)Math.Pow(2, 10);
            var disruptor = new Disruptor<EventType>(
                () => new EventType(),
                //ringSize,
                new SingleThreadedClaimStrategy(ringSize),
                new SleepingWaitStrategy(),
                TaskScheduler.Default
            );

            var ringBuffer = disruptor.RingBuffer;

            var deserialize = GetDeserializers(parallelism);
            var groupIntoRequests = GetRequestBuilders(listsPerRequest);

            disruptor.HandleEventsWith(deserialize)
                .Then(groupIntoRequests);

            var barrierUntilRequestsAreGrouped = disruptor.After(groupIntoRequests).AsSequenceBarrier();

            var executeRequests = GetRequestExecutors(ringBuffer, barrierUntilRequestsAreGrouped);

            disruptor.HandleEventsWith(executeRequests);

            var configuredRingBuffer = disruptor.Start();

            // There is a bug in the Disruptor code that prevents custom EventProcessors from running automatically
            // so we start them manually. If they were to be started twice, it would throw an exception. 
            foreach(var requestExecutor in executeRequests)
            {
                TaskExtensions.CreateNewLongRunningTask(() => requestExecutor.Run(), (ex) => Assert.Fail(ex.StackTrace));
            }

            var eventPublisher = new EventPublisher<EventType>(configuredRingBuffer);

            var messages = FakeDataGenerator.Generate(numberOfTodoLists, numberOfUpdates, maxNumberOfItemsPerList);

            Console.WriteLine("");
            Console.WriteLine("===========================");
            Console.WriteLine("");

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < messages.Length; i++)
            {
                eventPublisher.PublishEvent((@event, sequence) => {
                    @event.IncomingMessage = messages[i];
                    return @event;
                });
            }
            await Task.Delay(new TimeSpan(0,0,0,0,3000));
            //disruptor.Shutdown();
            timer.Stop();

            Console.WriteLine("");
            Console.WriteLine("===========================");
            Console.WriteLine("");
            Console.WriteLine("Took: " + timer.ElapsedMilliseconds + " ms");
        }
        
     

        private ParallelEventHandler<EventType>[] GetDeserializers(int parallelism)
        {
            Action<EventType, long, bool> deserializeAction = (@event, sequence, isEndOfBatch) =>
            {
                @event.IncomingMessage.Content = JsonConvert.DeserializeObject<IncomingMessageContent>(@event.IncomingMessage.ContentJson);
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
                        .Where(x => x.SyncType == SyncType.Delete)
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
                        Content = from todoList in content.TodoLists.Where(x => x.SyncType == SyncType.CreateOrUpdate)
                                  join lineItem in content.LineItems.Where(x => x.SyncType == SyncType.CreateOrUpdate)
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
                        Content = from todoList in content.TodoLists.Where(x => x.SyncType == SyncType.CreateOrUpdate)
                                  join lineItem in content.LineItems.Where(x => x.SyncType == SyncType.Delete)
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


        private IEventProcessor[] GetRequestExecutors (RingBuffer<EventType> ringBuffer, ISequenceBarrier sequenceBarrier)
        {
            var createOrUpdateRequestExecutor = new RequestExecutor<EventType, OutgoingRequest>(
                (@event) => {
                    LogRequest("cr", @event.CreateOrUpdateTodoListRequest);
                    return @event.CreateOrUpdateTodoListRequest;
                }
            );
            var deleteRequestExecutor = new RequestExecutor<EventType, OutgoingRequest>(
                (@event) => {
                    LogRequest("del", @event.CreateOrUpdateTodoListRequest);
                    return @event.CreateOrUpdateTodoListRequest;
                }
            );
            var removeItemsRequestExecutor = new RequestExecutor<EventType, OutgoingRequest>(
                (@event) => {
                    LogRequest("rm", @event.CreateOrUpdateTodoListRequest);
                    return @event.CreateOrUpdateTodoListRequest;
                }
            );

            return new IEventProcessor[] {
                new AsyncEventProcessor<EventType>(
                    ringBuffer, sequenceBarrier, new SpinLock(), (ex) => Assert.Fail(ex.StackTrace),
                    createOrUpdateRequestExecutor
                ),
                new AsyncEventProcessor<EventType>(
                    ringBuffer, sequenceBarrier, new SpinLock(), (ex) => Assert.Fail(ex.StackTrace),
                    deleteRequestExecutor
                ),
                new AsyncEventProcessor<EventType>(
                    ringBuffer, sequenceBarrier, new SpinLock(), (ex) => Assert.Fail(ex.StackTrace),
                    removeItemsRequestExecutor
                ),
            };
        }

        private void LogRequest(string prefix, OutgoingRequest request)
        {
            if (request == null)
                return;
            var content = request.Content;
            var ss = String.Join(",\n", content.Select(list => list.Id + "  " + list.Version + "   "));
            Console.WriteLine(prefix + ": " + ss);
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
