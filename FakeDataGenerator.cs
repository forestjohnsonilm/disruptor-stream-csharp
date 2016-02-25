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
    public class FakeDataGenerator
    {
        public static IncomingMessage[] Generate(int numberOfTodoLists, int numberOfUpdates, int maxItemsPerList)
        {
            var aggregateIds = new Guid[numberOfTodoLists];
            for (var i = 0; i < numberOfTodoLists; i++)
            {
                aggregateIds[i] = Guid.NewGuid();
            }

            var messages = new IncomingMessage[numberOfUpdates];
            var todoListVersions = new Dictionary<Guid, int>();

            var random = new Random();
            for (var i = 0; i < numberOfUpdates; i++)
            {
                // Choose a random todo list Id for this message
                var todoListId = aggregateIds[random.Next() % numberOfTodoLists];

                var todoListVersion = 1;
                if (!todoListVersions.ContainsKey(todoListId))
                {
                    todoListVersions.Add(todoListId, todoListVersion);
                }
                else
                {
                    todoListVersion = ++todoListVersions[todoListId];
                }

                var itemCount = random.Next() % maxItemsPerList;
                var lineItems = new List<IncomingLineItem>();
                for (var j = 0; j < itemCount; j++)
                {
                    lineItems.Add(new IncomingLineItem()
                    {
                        Id = j,
                        TodoListId = todoListId,
                        Version = todoListVersion,
                        SortOrder = itemCount - j,
                        Done = random.NextDouble() > 0.5d,
                        TaskDescription = Guid.NewGuid().ToString(),
                        SyncType = random.NextDouble() > 0.5d ? SyncType.CreateOrUpdate : SyncType.Delete
                    });
                }

                var content = new IncomingMessageContent()
                {
                    TodoLists = new List<IncomingTodoList>() {
                        new IncomingTodoList(
                            todoListId,
                            todoListVersion,
                            Guid.NewGuid().ToString(),
                            Guid.NewGuid().ToString(),
                            random.NextDouble() > 0.5d ? SyncType.CreateOrUpdate : SyncType.Delete
                        )
                    },
                    LineItems = lineItems
                };

                var json = JsonConvert.SerializeObject(content);
                messages[i] = new IncomingMessage()
                {
                    ContentJson = json
                };

                //Console.WriteLine(json);
            }

            return messages;
        }
    }
}
