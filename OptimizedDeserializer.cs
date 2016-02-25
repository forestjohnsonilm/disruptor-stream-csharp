using System;
using System.Linq;
using NUnit.Framework;
using Disruptor.Dsl;
using Disruptor;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.IO;

namespace DisruptorTest
{
    
    public class OptimizedDeserializer
    {
        private enum LastArrayProp
        {
            Unknown,
            TodoLists,
            LineItems
        }

        public IncomingMessageContent Deserialize(string jsonString) {
            var reader = new JsonTextReader(new StringReader(jsonString));

            var toReturn = new IncomingMessageContent();
            var todoLists = new List<IncomingTodoList>();
            var lineItems = new List<IncomingLineItem>();
            IncomingTodoList currentTodoList = null;
            IncomingLineItem currentLineItem = null;
            var currentProperty = string.Empty;
            var lastArrayProperty = LastArrayProp.Unknown;

            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.PropertyName)
                    currentProperty = reader.Value.ToString();

                if (reader.TokenType == JsonToken.EndArray)
                {
                    lastArrayProperty = LastArrayProp.Unknown;
                }

                if (lastArrayProperty == LastArrayProp.TodoLists && reader.TokenType == JsonToken.EndObject)
                {
                    todoLists.Add(currentTodoList);
                }
                if (lastArrayProperty == LastArrayProp.LineItems && reader.TokenType == JsonToken.EndObject)
                {
                    lineItems.Add(currentLineItem);
                }

                if (reader.TokenType == JsonToken.StartObject
                    && (lastArrayProperty == LastArrayProp.TodoLists || currentProperty == "TodoLists"))
                {
                    lastArrayProperty = LastArrayProp.TodoLists;
                    currentTodoList = new IncomingTodoList();
                }

                if (reader.TokenType == JsonToken.StartObject
                    && (lastArrayProperty == LastArrayProp.LineItems || currentProperty == "LineItems"))
                {
                    lastArrayProperty = LastArrayProp.LineItems;
                    currentLineItem = new IncomingLineItem();
                }

                if (lastArrayProperty == LastArrayProp.TodoLists && reader.TokenType == JsonToken.Integer)
                {
                    if (currentProperty == "RequestType")
                        currentTodoList.RequestType = (RequestType)Int32.Parse(reader.Value.ToString());
                    if (currentProperty == "Version")
                        currentTodoList.Version = Int32.Parse(reader.Value.ToString());
                }
                if (lastArrayProperty == LastArrayProp.TodoLists && reader.TokenType == JsonToken.String)
                {
                    if (currentProperty == "Id")
                        currentTodoList.Id = Guid.Parse(reader.Value.ToString());
                    if (currentProperty == "Title")
                        currentTodoList.Title = reader.Value.ToString();
                    if (currentProperty == "Description")
                        currentTodoList.Description = reader.Value.ToString();
                }

                if (lastArrayProperty == LastArrayProp.LineItems && reader.TokenType == JsonToken.Integer)
                {
                    if (currentProperty == "RequestType")
                        currentLineItem.RequestType = (RequestType)Int32.Parse(reader.Value.ToString());
                    if (currentProperty == "Version")
                        currentLineItem.Version = Int32.Parse(reader.Value.ToString());
                    if (currentProperty == "Id")
                        currentLineItem.Id = Int32.Parse(reader.Value.ToString());
                    if (currentProperty == "SortOrder")
                        currentLineItem.SortOrder = Int32.Parse(reader.Value.ToString());
                }
                if (lastArrayProperty == LastArrayProp.LineItems && reader.TokenType == JsonToken.Boolean)
                {
                    currentLineItem.Done = Boolean.Parse(reader.Value.ToString());
                }
                if (lastArrayProperty == LastArrayProp.LineItems && reader.TokenType == JsonToken.String)
                {
                    if (currentProperty == "TodoListId")
                        currentLineItem.TodoListId = Guid.Parse(reader.Value.ToString());
                    if (currentProperty == "TaskDescription")
                        currentLineItem.TaskDescription = reader.Value.ToString();
                }
            }

            toReturn.LineItems = lineItems;
            toReturn.TodoLists = todoLists;

            return toReturn;
        }
    }
}
