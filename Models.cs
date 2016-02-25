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
        public TodoList() { }
        public TodoList(Guid id)
        {
            Id = id;
            LineItems = Enumerable.Empty<LineItem>();
        }
        public TodoList(Guid id, IEnumerable<LineItem> lineItems)
        {
            Id = id;
            LineItems = lineItems;
        }
        public TodoList(Guid id, int version, string title, string description, IEnumerable<LineItem> lineItems)
        {
            Id = id;
            Version = version;
            Title = title;
            Description = description;
            LineItems = lineItems;
        }

        public IEnumerable<LineItem> LineItems;

        public static TodoList Merge(IEnumerable<TodoList> lists)
        {
            var newest = lists.OrderByDescending(x => x.Version).First();
            return new TodoList(
                newest.Id, newest.Version, newest.Title, newest.Description,
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
    }

    public class IncomingTodoList : BaseTodoList
    {
        public RequestType RequestType;

        public IncomingTodoList() { }
        public IncomingTodoList(Guid id, int version, string title, string description, RequestType requestType)
        {
            Id = id;
            Version = version;
            Title = title;
            Description = description;
            RequestType = requestType;
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
        public RequestType RequestType;
        public Guid TodoListId;
    }

    public enum RequestType
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
