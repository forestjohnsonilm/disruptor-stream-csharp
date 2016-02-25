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
    public class RequestBuilder<T> : IEventHandler<T>
    {
        public OutgoingRequest RequestBeingBuilt;

        private readonly Func<T, OutgoingRequest> _getRequest;
        private readonly Action<T, OutgoingRequest> _setRequest;
        private readonly int _maxTodoListsPerRequest;

        public RequestBuilder(
                Func<T, OutgoingRequest> getRequest,
                Action<T, OutgoingRequest> setRequest,
                int maxTodoListsPerRequest
            )
        {
            RequestBeingBuilt = new OutgoingRequest();
            _getRequest = getRequest;
            _setRequest = setRequest;
            _maxTodoListsPerRequest = maxTodoListsPerRequest;
        }

        public void OnNext(T @event, long sequence, bool isEndOfBatch)
        {
            var newRequest = _getRequest(@event);

            if (isEndOfBatch)
            {
                _setRequest(@event, RequestBeingBuilt.MergeWith(newRequest));
                RequestBeingBuilt = new OutgoingRequest();

            }
            else if (newRequest.Content.Count() + RequestBeingBuilt.Content.Count() > _maxTodoListsPerRequest)
            {
                _setRequest(@event, RequestBeingBuilt);
                RequestBeingBuilt = newRequest;
            }
            else
            {
                RequestBeingBuilt = RequestBeingBuilt.MergeWith(newRequest);
            }
        }
    }
}
