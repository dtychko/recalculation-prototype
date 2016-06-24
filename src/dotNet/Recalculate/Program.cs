using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
using ProtoBuf;
using RabbitMQ.Client;

namespace Recalculate
{
	internal static class Constants
	{
		public static readonly string RabbitMqHostName = "192.168.99.100";
		public static readonly string MetricSetupEventsQueueName = "metric_setup_events";

		public static readonly string MetricSetupsFileDirectory = ".";
		public static readonly string MetricSetupsFileName = "metric_setups.json";
	}

	#region Model

	[ProtoContract]
	internal class MetricSetup
	{
		[ProtoMember(1, IsRequired = true)]
		public int Id { get; set; }

		[ProtoMember(2, IsRequired = true)]
		public int MetricId { get; set; }

		[ProtoMember(3, IsRequired = true)]
		public string EntityTypes { get; set; }
	}

	internal enum Modification
	{
		None = 0,
		Added = 1,
		Updated = 2,
		Deleted = 3
	}

	[ProtoContract]
	internal class MetricSetupChangedEvent
	{
		[ProtoMember(1, IsRequired = true)]
		public MetricSetup MetricSetup { get; set; }

		[ProtoMember(2, IsRequired = true)]
		public Modification Modification { get; set; }

		[ProtoMember(3, IsRequired = true)]
		public int AccountId { get; set; }
	}

	#endregion

	class Program
	{
		static void Main()
		{
			var factory = new ConnectionFactory {HostName = Constants.RabbitMqHostName};

			using (var connection = factory.CreateConnection())
			{
				using (var channel = connection.CreateModel())
				{
					channel.QueueDeclare(
						queue: Constants.MetricSetupEventsQueueName,
						durable: false,
						exclusive: false,
						autoDelete: false,
						arguments: null);

					using (var service = new MetricSetupService(Constants.MetricSetupsFileDirectory, Constants.MetricSetupsFileName))
					{
						service.MetricSetupChanged += (sender, @event) =>
						{
							channel.BasicPublish(
								exchange: "",
								routingKey: Constants.MetricSetupEventsQueueName,
								basicProperties: null,
								body: new ProtoBufSerializer().Serialize(@event));

							Console.WriteLine($" [x] Sent {JsonConvert.SerializeObject(@event)} \n");
						};
						service.Init();

						Console.ReadLine();
					}
				}
			}
		}
	}

	internal class ProtoBufSerializer
	{
		public byte[] Serialize<T>(T obj)
		{
			using (var stream = new MemoryStream())
			{
				Serializer.Serialize(stream, obj);
				return stream.ToArray();
			}
		}
	}

	internal class MetricSetupService : IDisposable
	{
		public event EventHandler<MetricSetupChangedEvent> MetricSetupChanged;

		private readonly string _path;
		private readonly string _fileName;
		private FileSystemWatcher _watcher;
		private IDictionary<int, MetricSetup[]> _metricSetupsMap;

		public MetricSetupService(string path, string fileName)
		{
			_path = path;
			_fileName = fileName;
		}

		public void Init()
		{
			_watcher = new FileSystemWatcher(_path, _fileName)
			{
				EnableRaisingEvents = true
			};
			_watcher.Changed += WatcherOnChanged;
			_metricSetupsMap = GetActualMetricSetupsMap();
		}

		private void WatcherOnChanged(object sender, FileSystemEventArgs fileSystemEventArgs)
		{
			Thread.Sleep(100);

			var actualMetricSetupsMap = GetActualMetricSetupsMap();
			var accounts = _metricSetupsMap.Keys.Concat(actualMetricSetupsMap.Keys).Distinct();

			foreach (var accountId in accounts)
			{
				var metricSetups = _metricSetupsMap.ContainsKey(accountId) ? _metricSetupsMap[accountId] : new MetricSetup[0];
				var actualMetricSetups = actualMetricSetupsMap.ContainsKey(accountId) ? actualMetricSetupsMap[accountId] : new MetricSetup[0];

				var newSetups = actualMetricSetups.Where(x => metricSetups.All(y => y.Id != x.Id));
				var deltas = metricSetups
					.Select(x => new
					{
						Old = x,
						New = actualMetricSetups.FirstOrDefault(y => y.Id == x.Id)
					});

				foreach (var setup in newSetups)
				{
					OnMetricSetupChanged(accountId, setup, Modification.Added);
				}

				foreach (var delta in deltas)
				{
					if (delta.New == null)
					{
						OnMetricSetupChanged(accountId, delta.Old, Modification.Deleted);
					}
					else if (delta.New.MetricId != delta.Old.MetricId ||
							 delta.New.EntityTypes != delta.Old.EntityTypes)
					{
						OnMetricSetupChanged(accountId, delta.New, Modification.Updated);
					}
				}
			}

			_metricSetupsMap = actualMetricSetupsMap;
		}

		private IDictionary<int, MetricSetup[]> GetActualMetricSetupsMap()
		{
			var stream = File.OpenRead(Path.Combine(_path, _fileName));

			using (var reader = new StreamReader(stream))
			{
				return JsonConvert.DeserializeAnonymousType(reader.ReadToEnd(),
					new[] {new {AccountId = default(int), Items = new MetricSetup[0]}})
					.ToDictionary(x => x.AccountId, x => x.Items);
			}
		}

		private void OnMetricSetupChanged(int accountId, MetricSetup metricSetup, Modification modification)
		{
			MetricSetupChanged?.Invoke(this,
				new MetricSetupChangedEvent
				{
					AccountId = accountId,
					MetricSetup = metricSetup,
					Modification = modification
				});
		}

		public void Dispose()
		{
			_watcher?.Dispose();
		}
	}
}
