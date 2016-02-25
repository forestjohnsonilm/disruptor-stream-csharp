using System;
using System.Linq;
using NUnit.Framework;
using Disruptor.Dsl;
using Disruptor;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Collections.Concurrent;

namespace DisruptorTest
{
    public class MockExternalService<TPayload, TResult>
    {
        private readonly BlockingCollection<ExternalCall> _completionSources = new BlockingCollection<ExternalCall>();

        private readonly Random _randomNetworkLatency = new Random();

        private int _baseLatencyMs = 20;

        private class ExternalCall
        {
            public DateTime TimeToComplete;
            public TaskCompletionSource<TResult> CompletionSource;
            public Action<TResult> Callback;
        }

        public void Run()
        {
            while(true)
            {
                var next = _completionSources.Take();
                if(next.TimeToComplete > DateTime.UtcNow)
                {
                    Thread.Sleep(0);
                }

                if(next.CompletionSource != null)
                {
                    next.CompletionSource.TrySetResult(default(TResult));
                }
                else
                {
                    next.Callback(default(TResult));
                }
                
            }
        }

        public Task<TResult> Call(TPayload payload)
        {
            var randomLatency = new TimeSpan(0, 0, 0, 0, _baseLatencyMs + (_randomNetworkLatency.Next() % _baseLatencyMs));
            var externalCall = new ExternalCall()
            {
                TimeToComplete = DateTime.UtcNow + randomLatency,
                CompletionSource = new TaskCompletionSource<TResult>()
            };

            _completionSources.Add(externalCall);

            return externalCall.CompletionSource.Task;
        }

        public void CallWithCallback(TPayload payload, Action<TResult> callback)
        {
        var randomLatency = new TimeSpan(0, 0, 0, 0, _baseLatencyMs + (_randomNetworkLatency.Next() % _baseLatencyMs));
        var externalCall = new ExternalCall()
            {
                TimeToComplete = DateTime.UtcNow + randomLatency,
                Callback = callback
            };

            _completionSources.Add(externalCall);
        }
    }
}
