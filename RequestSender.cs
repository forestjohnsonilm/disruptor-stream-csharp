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
        private readonly MockExternalService<TPayload, int> _mockService;
        private readonly Action<Exception> _onException;
        private readonly Func<TEvent, TPayload> _getPayload;
        private readonly RequestSenderMode _mode;

        public RequestSender(
                Func<TEvent, TPayload> getPayload, 
                MockExternalService<TPayload, int> mockService,
                RequestSenderMode mode,
                Action<Exception> onException
            )
        {
            _mockService = mockService;
            _getPayload = getPayload;
            _onException = onException;
            _mode = mode;
        } 
        
        public void OnNext(TEvent @event, long sequence, bool endOfBatch, CancellationToken cancellationToken, Action<long> callback)
        {
            var payload = _getPayload(@event);
            if(payload != null)
            {
                if(_mode == RequestSenderMode.Task)
                {
                    AsyncExtensions.FireAndForget(async () => {
                        await _mockService.Call(_getPayload(@event));
                        callback(sequence);
                    }, _onException);
                }
                else
                {
                    _mockService.CallWithCallback(_getPayload(@event), (unused) => callback(sequence));
                }
            }
            else
            {
                callback(sequence);
            }
        }
    }

    public enum RequestSenderMode
    {
        Callback = 0,
        Task = 1
    }
}
