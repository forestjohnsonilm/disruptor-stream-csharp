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
    public class RequestSender<TEvent, TPayload> : AsyncEventProcessorImplementation<TEvent>
    {
        private MockExternalService<TPayload, int> _mockService;

        private readonly Func<TEvent, TPayload> _getPayload;
        public RequestSender(Func<TEvent, TPayload> getPayload, MockExternalService<TPayload, int> mockService)
        {
            _mockService = mockService;
            _getPayload = getPayload;
        }

        public bool ShouldSpawnTaskFor(TEvent @event, long sequence, bool endOfBatch)
        {
            return _getPayload(@event) != null;
        }

        public async Task OnNext(TEvent @event, long sequence, bool endOfBatch, CancellationToken cancellationToken)
        {
            await _mockService.Call(_getPayload(@event));
        }
    }
}
