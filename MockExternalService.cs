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
    public class MockExternalService<TPayload>
    {
        public async Task Call(TPayload payload)
        {
            await Task.Delay(new TimeSpan(0,0,0,0,200));
        }
    }
}
