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
    public class ParallelEventHandler<T> : IEventHandler<T>
    {
        private readonly int _partitionId;
        private readonly int _partitionCount;
        private readonly Action<T, long, bool> _action;

        public static ParallelEventHandler<T>[] Group(int parallelism, Action<T, long, bool> action)
        {
            var toReturn = new ParallelEventHandler<T>[parallelism];
            for(var i = 0; i < parallelism; i++)
            {
                toReturn[i] = new ParallelEventHandler<T>(i, parallelism, action);
            }

            return toReturn;
        }

        protected ParallelEventHandler(int partitionId, int partitionCount, Action<T, long, bool> action)
        {
            _partitionId = partitionId;
            _partitionCount = partitionCount;
            _action = action;
        }

        public void OnNext(T @event, long sequence, bool isEndOfBatch)
        {
            if (sequence % _partitionCount == _partitionId)
            {
                _action(@event, sequence, isEndOfBatch);
            }
        }
    }
}
