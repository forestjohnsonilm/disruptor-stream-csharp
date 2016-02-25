using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Disruptor;
using Disruptor.Dsl;
using System.Threading;

namespace DisruptorTest
{
    public static class TaskExtensions 
    {
        public static void CreateNewLongRunningTask(Action action, Action<Exception> onException)
        {
            Task.Factory.StartNew(
                () => {
                    try
                    {
                        action();
                    }
                    catch (Exception ex)
                    {
                        onException(ex);
                    }
                },
                CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default
            );
        }
        public static void FireAndForget(Func<Task> asyncAction, Action<Exception> onException)
        {
            Task.Run(async () => {
                try
                {
                    await asyncAction();
                }
                catch (Exception ex)
                {
                    onException(ex);
                }
            });
        }
    }
}
