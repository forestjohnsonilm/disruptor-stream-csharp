using System;
using System.Linq;
using NUnit.Framework;
using Disruptor.Dsl;
using Disruptor;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace DisruptorTest
{
    [TestFixture]
    public class DisruptorExample
    {

        [Test]
        public void DemonstrateDisruptor()
        {
            var parallelism = 4;
            var listsPerRequest = 10;

            var disruptor = new Disruptor<EventType>(() => new EventType(), (int)Math.Pow(2, 10), TaskScheduler.Default);

            var ringBuffer = disruptor.RingBuffer;

            var deserialize = GetDeserializers(parallelism);
            var groupIntoRequests = GetRequestBuilders(listsPerRequest);

            disruptor.HandleEventsWith(deserialize)
                .Then(groupIntoRequests);

            var barrierUntilRequestsAreGrouped = disruptor.After(groupIntoRequests).AsSequenceBarrier();

            var executeRequests = GetRequestExecutors(ringBuffer, barrierUntilRequestsAreGrouped);

            disruptor.HandleEventsWith(executeRequests);

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
                                  select new TodoList(todoList, lineItems)
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
            return new IEventProcessor[] {
                new AsyncEventProcessor<EventType>(ringBuffer, sequenceBarrier, new SpinLock(),
                    new RequestExecutor<EventType, OutgoingRequest>((@event) => @event.CreateOrUpdateTodoListRequest)),

                new AsyncEventProcessor<EventType>(ringBuffer, sequenceBarrier, new SpinLock(),
                    new RequestExecutor<EventType, OutgoingRequest>((@event) => @event.DeleteTodoListsRequest)),

                new AsyncEventProcessor<EventType>(ringBuffer, sequenceBarrier, new SpinLock(),
                    new RequestExecutor<EventType, OutgoingRequest>((@event) => @event.RemoveLineItemsRequest)),
            };
        }

        private class RequestExecutor<TEvent, TPayload> : AsyncEventProcessorImplementation<TEvent>
        {
            private readonly Func<TEvent, TPayload> _getPayload;
            public RequestExecutor(Func<TEvent, TPayload> getPayload)
            {
                _getPayload = getPayload;
            }

            public async Task OnNext(TEvent @event, long sequence, bool endOfBatch, CancellationToken cancellationToken)
            {
                var payload = _getPayload(@event);
                if(payload != null)
                {
                    await MockExternalService<TPayload>.Call(payload);
                }
            }
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
