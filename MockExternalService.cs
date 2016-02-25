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

        private class ExternalCall
        {
            public DateTime TimeToComplete;
            public TaskCompletionSource<TResult> CompletionSource;
        }

        public void Run()
        {
            while(true)
            {
                var next = _completionSources.Take();
                if(next.TimeToComplete > DateTime.UtcNow)
                {
                    Thread.Sleep(10);
                }
                next.CompletionSource.TrySetResult(default(TResult));
            }
        }

        public Task<TResult> Call(TPayload payload)
        {
            var externalCall = new ExternalCall()
            {
                TimeToComplete = DateTime.UtcNow + new TimeSpan(0, 0, 0, 0, 100 + (_randomNetworkLatency.Next() % 100)),
                CompletionSource = new TaskCompletionSource<TResult>()
            };

            _completionSources.Add(externalCall);

            return externalCall.CompletionSource.Task;
        }
    }
}
