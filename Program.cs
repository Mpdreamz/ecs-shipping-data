using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.CommonSchema;
using Elastic.CommonSchema.Serilog;
using Elastic.Elasticsearch.Ephemeral;
using Nest;
using Serilog;
using Serilog.Sinks.Elasticsearch;
using Log = Serilog.Log;

namespace ShippingData
{
    class Program
    {
        static void Main(string[] args)
        {
            var clusterConfiguration = new EphemeralClusterConfiguration("7.9.0");
            using var cluster = new EphemeralCluster(clusterConfiguration);
            
            cluster.Start(TimeSpan.FromMinutes(2));
            var uri = cluster.NodesUris().First();


            var nestIndex = "logs-from-nest";
            Announce("NEST + Elastic.CommonSchema", nestIndex);
            var settings = new ConnectionSettings(uri).EnableDebugMode();
            var client = new ElasticClient(settings);
            

            var bulkAll = client.BulkAll(GetEcsEvents(10_000), b => b
                .Index(nestIndex)
                .Size(1_000)
                .RefreshOnCompleted()
                .BufferToBulk((bulk, items) => bulk.CreateMany(items))
            );
            bulkAll.Wait(TimeSpan.FromMinutes(2), r => Console.WriteLine("Indexed 1000 events"));
            
            Check(client, nestIndex);

            
            // Using Serilog ElasticsearchSink
            var sinkIndex = "logs-from-sink";
            Announce("Elastic.CommonSchema.Serilog", sinkIndex);
            var options = new ElasticsearchSinkOptions(uri)
            {
                CustomFormatter = new EcsTextFormatter(),
                IndexFormat = sinkIndex + "-{0:yyyy-MM-dd}",
                DetectElasticsearchVersion = true,
                BatchAction  = ElasticOpType.Create,
                BatchPostingLimit = 1_000,
            };
            var logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Elasticsearch(options)
                .CreateLogger();

            Log.Logger = logger;

            foreach (var e in GetEcsEvents(10_000))
                Log.Debug("Reusing {field1} by {another_field}: {message}", "some value", true, e.Message);            
            
            Log.CloseAndFlush();
            
            Check(client, $"{sinkIndex}-*");
            
        }

        public static void Announce(string method, string index)
        {
            Console.WriteLine();
            Console.WriteLine(new string('-', 30));
            Console.WriteLine($" Using {method} to send data into index {index}");
            Console.WriteLine(new string('-', 30));
        }
        
        public static void Check(IElasticClient client, string index)
        {
            var indices = Infer.Indices(index);
            var response = client.Indices.Refresh(indices);
            var search = client.Search<Base>(c => c.Size(1).Index(index));
            Console.WriteLine($"The index has {search.Total} logs");
            Console.WriteLine($"Last indexed message: {search.Documents.Last().Message}");
        }

        public static IEnumerable<Base> GetEcsEvents(int numberOfEvents)
        {
            for(var i = 0; i < numberOfEvents; i++)
                yield return new Base
                {
                    Timestamp = DateTimeOffset.UtcNow,
                    Message = $"Generated event {i} ",
                    Service = new Service { Name = "azure"},
                    Event = new Event { Module = "azure", Dataset = "azure.platformlogs"},
                };
        }
        
    }
}