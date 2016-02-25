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
    public class IncomingMessage
    {
        public string ContentJson;
        public IncomingMessageContent Content;
    }

    public class OutgoingRequest
    {
        public IEnumerable<TodoList> Content = Enumerable.Empty<TodoList>();

        public OutgoingRequest MergeWith(OutgoingRequest other)
        {
            return new OutgoingRequest()
            {
                Content = Merger<TodoList, Guid>
                    .GroupByIdAndMerge(Content.Concat(other.Content), (x) => x.Id, TodoList.Merge)
            };
        }
    }

    public class IncomingMessageContent
    {
        public IEnumerable<IncomingTodoList> TodoLists;
        public IEnumerable<IncomingLineItem> LineItems;
    }

    public class TodoList : BaseTodoList
    {
        public TodoList(Guid id) : base(id)
        {
            LineItems = Enumerable.Empty<LineItem>();
        }
        public TodoList(Guid id, IEnumerable<LineItem> lineItems) : base(id)
        {
            LineItems = lineItems;
        }
        public TodoList(BaseTodoList @base, IEnumerable<LineItem> lineItems)
            : base(@base.Id, @base.Version, @base.Title, @base.Description)
        {
            LineItems = lineItems;
        }

        public IEnumerable<LineItem> LineItems;

        public static TodoList Merge(IEnumerable<TodoList> lists)
        {
            return new TodoList(
                lists.OrderByDescending(x => x.Version).First(),
                Merger<LineItem, int>.GroupByIdAndMerge(
                    lists.SelectMany(x => x.LineItems),
                    (x) => x.Id,
                    (items) => items.OrderByDescending(x => x.Version).First()
                )
            );
        }
    }

    public class BaseTodoList
    {
        public Guid Id;
        public int Version;
        public string Title;
        public string Description;

        public BaseTodoList(Guid id)
        {
            Id = id;
        }
        public BaseTodoList(Guid id, int version, string title, string description)
        {
            Id = id;
            Version = version;
            Title = title;
            Description = description;
        }
    }

    public class IncomingTodoList : BaseTodoList
    {
        public SyncType SyncType;

        public IncomingTodoList(BaseTodoList @base, SyncType syncType)
            : base(@base.Id, @base.Version, @base.Title, @base.Description)
        {
            SyncType = syncType;
        }
    }

    public class LineItem
    {
        public int Id;
        public int Version;
        public int SortOrder;
        public bool Done;
        public string TaskDescription;
    }

    public class IncomingLineItem : LineItem
    {
        public SyncType SyncType;
        public Guid TodoListId;
    }

    public enum SyncType
    {
        CreateOrUpdate = 0,
        Delete = 1
    }

    public class Merger<TObject, TId>
            where TId : IEquatable<TId>
    {
        public static IEnumerable<TObject> GroupByIdAndMerge(
                IEnumerable<TObject> collection,
                Func<TObject, TId> idSelector,
                Func<IEnumerable<TObject>, TObject> merger
            )
        {
            return collection
                    .GroupBy(idSelector)
                    .Select(merger);
        }
    }
}
