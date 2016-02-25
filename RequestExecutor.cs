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
    public class RequestExecutor<TEvent, TPayload> : AsyncEventProcessorImplementation<TEvent>
    {
        private MockExternalService<TPayload> _mockService = new MockExternalService<TPayload>();

        private readonly Func<TEvent, TPayload> _getPayload;
        public RequestExecutor(Func<TEvent, TPayload> getPayload)
        {
            _getPayload = getPayload;
        }

        public bool ShouldSpawnTaskFor(TEvent @event, long sequence, bool endOfBatch)
        {
            return _getPayload(@event) != null;
        }

        public async Task OnNext(TEvent @event, long sequence, bool endOfBatch, CancellationToken cancellationToken)
        {
            var payload = _getPayload(@event);

            var content = (payload as OutgoingRequest).Content;
            var ss = String.Join(",\n", content.Select(list => list.Id + "  " + list.Version + "   "));
            Console.WriteLine("service call: " + ss);

            await _mockService.Call(payload);

            Console.WriteLine("DONE: " + ss);
        }

      
    }
}
