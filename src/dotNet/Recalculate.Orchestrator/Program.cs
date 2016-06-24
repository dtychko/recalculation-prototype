using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using ProtoBuf;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Recalculate.Orchestrator
{
	internal static class Constants
	{
		public static readonly string RabbitMqHostName = "192.168.99.100";
		public static readonly string MetricSetupEventsQueueName = "metric_setup_events";

		public static readonly ushort RabbitMqPrefetchCount = 50;

		public static readonly int PartitionSize = 100;

		public static readonly TimeSpan TargetReadingTime = TimeSpan.FromMilliseconds(1000);
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

	[ProtoContract]
	internal class CalculateMetricCommand
	{
		[ProtoMember(1, IsRequired = true)]
		public int AccountId { get; set; }

		[ProtoMember(2, IsRequired = true)]
		public MetricSetup MetricSetup { get; set; }

		[ProtoMember(3, IsRequired = true)]
		public int[] TargetIds { get; set; }

		[ProtoMember(4, IsRequired = true)]
		public string CommandId { get; set; }
	}

	#endregion

	internal class Orchestrator : IDisposable
	{
		private readonly IConnection _connection;
		private readonly IModel _channel;
		private readonly IListener[] _listeners;

		public static IDisposable Run(IListenerProvider listenerProvider)
		{
			return new Orchestrator(listenerProvider);
		}

		private Orchestrator(IListenerProvider listenerProvider)
		{
			var factory = new ConnectionFactory { HostName = Constants.RabbitMqHostName };
			_connection = factory.CreateConnection();
			_channel = _connection.CreateModel();
			_listeners = listenerProvider.GetAll(_channel).ToArray();

			foreach (var listener in _listeners)
			{
				listener.Start();
			}
		}

		public void Dispose()
		{
			foreach (var listener in _listeners)
			{
				listener.Dispose();
			}

			_channel.Dispose();
			_connection.Dispose();
		}
	}

	internal interface IListenerProvider
	{
		IEnumerable<IListener> GetAll(IModel channel);
	}

	internal class ListenerProvider : IListenerProvider
	{
		public IEnumerable<IListener> GetAll(IModel channel)
		{
			yield return
				new Listener<MetricSetupChangedEvent>(channel, Constants.MetricSetupEventsQueueName,
					new MetricSetupChangedEventHandler(
						new CalculateMetricCommandPublisherFactory(channel)));
		}
	}

	internal interface IListener : IDisposable
	{
		void Start();
	}

	internal abstract class ListenerBase<TMessage> : IListener
	{
		private readonly IModel _channel;
		private readonly string _queueName;
		private string _consumerTag;
		private CancellationTokenSource _cancellationTokenSource;
		private bool _isDisposed;

		protected ListenerBase(IModel channel, string queueName)
		{
			_channel = channel;
			_queueName = queueName;
		}

		public void Start()
		{
			_channel.QueueDeclare(
				queue: _queueName,
				durable: false,
				exclusive: false,
				autoDelete: false,
				arguments: null);

			_cancellationTokenSource = new CancellationTokenSource();

			var consumer = new EventingBasicConsumer(_channel);
			consumer.Received += OnReceived;
			_consumerTag = _channel.BasicConsume(
				queue: _queueName,
				noAck: false,
				consumer: consumer);
		}

		private async void OnReceived(object sender, BasicDeliverEventArgs eArgs)
		{
			try
			{
				await OnReceivedAsync(eArgs, _cancellationTokenSource.Token);
			}
			catch (OperationCanceledException)
			{
			}
		}

		private async Task OnReceivedAsync(BasicDeliverEventArgs eArgs, CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();

			var message = new ProtoBufSerializer().Deserialize<TMessage>(eArgs.Body);
			Console.WriteLine($" [x] Received {JsonConvert.SerializeObject(message)} \n");

			var handled = true;

			cancellationToken.ThrowIfCancellationRequested();

			try
			{
				await ProcessMessageAsync(message, cancellationToken);
			}
			catch (Exception ex) when (!(ex is TaskCanceledException))
			{
				handled = false;
			}

			cancellationToken.ThrowIfCancellationRequested();

			try
			{
				if (handled)
				{
					_channel.BasicAck(eArgs.DeliveryTag, false);
				}
				else
				{
					_channel.BasicNack(eArgs.DeliveryTag, false, true);
				}
			}
			catch (AlreadyClosedException)
			{
			}
		}

		protected abstract Task ProcessMessageAsync(TMessage message, CancellationToken cancellationToken);

		public void Dispose()
		{
			if (_isDisposed)
			{
				return;
			}

			_isDisposed = true;

			if (_consumerTag != null)
			{
				_channel.BasicCancel(_consumerTag);
			}

			if (_cancellationTokenSource != null)
			{
				_cancellationTokenSource.Cancel();
				_cancellationTokenSource.Dispose();
			}
		}
	}

	internal class Listener<TMessage> : ListenerBase<TMessage>
	{
		private readonly IMessageHandler<TMessage> _handler;

		public Listener(IModel channel, string queueName, IMessageHandler<TMessage> handler)
			: base(channel, queueName)
		{
			_handler = handler;
		}

		protected override async Task ProcessMessageAsync(TMessage message, CancellationToken cancellationToken)
		{
			await _handler.HandleAsync(message, cancellationToken);
		}
	}

	internal interface IMessageHandler<in TMessage>
	{
		Task HandleAsync(TMessage message, CancellationToken cancellationToken);
	}

	internal class MetricSetupChangedEventHandler : IMessageHandler<MetricSetupChangedEvent>
	{
		private readonly CalculateMetricCommandPublisherFactory _publisherFactory;

		public MetricSetupChangedEventHandler(CalculateMetricCommandPublisherFactory publisherFactory)
		{
			_publisherFactory = publisherFactory;
		}

		public async Task HandleAsync(MetricSetupChangedEvent @event, CancellationToken cancellationToken)
		{
			var targetIds = await new TargetProvider().GetTargets(@event.AccountId, @event.MetricSetup);
			var batches = new Partitioner().Create(targetIds);

			foreach (var batch in batches)
			{
				var command = new CalculateMetricCommand
				{
					CommandId = Guid.NewGuid().ToString(),
					AccountId = @event.AccountId,
					MetricSetup = @event.MetricSetup,
					TargetIds = batch.ToArray()
				};

				var publisher = _publisherFactory.GetPublisher(@event.AccountId);
				publisher.Publish(command);

				Log(command);
			}
		}

		private static void Log(CalculateMetricCommand command)
		{
			var obj = JsonConvert.SerializeObject(
				new
				{
					CommandId = command.CommandId.ToString().Substring(0, 8),
					command.AccountId,
					command.MetricSetup,
					TargetIdsCount = command.TargetIds.Length
				});
			Console.WriteLine($" [x] Sent {obj} \n");
		}
	}

	internal class CalculateMetricCommandPublisherFactory
	{
		private readonly IModel _channel;

		public CalculateMetricCommandPublisherFactory(IModel channel)
		{
			_channel = channel;
		}

		public Publisher GetPublisher(int accountId)
		{
			return new Publisher(_channel, GetQueueName(accountId));
		}

		private static string GetQueueName(int accountId) => $"calc_requests_for_account_{accountId}";
	}

	internal class Publisher
	{
		private readonly IModel _channel;
		private readonly string _queueName;

		public Publisher(IModel channel, string queueName)
		{
			_channel = channel;
			_queueName = queueName;
			_channel.QueueDeclare(
				queue: _queueName,
				durable: false,
				exclusive: false,
				autoDelete: false,
				arguments: null);
		}

		public void Publish<TMessage>(TMessage message)
		{
			_channel.BasicPublish(
				exchange: "",
				routingKey: _queueName,
				basicProperties: null,
				body: new ProtoBufSerializer().Serialize(message));
		}
	}

	internal class Program
	{
		static void Main()
		{
			using (Orchestrator.Run(new ListenerProvider()))
			{
				Console.ReadLine();
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

		public T Deserialize<T>(byte[] data)
		{
			using (var stream = new MemoryStream(data))
			{
				return Serializer.Deserialize<T>(stream);
			}
		}
	}

	internal class TargetProvider
	{
		private static readonly IDictionary<string, int> EntityRegistry =
			new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
			{
				{"Epic", 10},
				{"Feature", 100},
				{"UserStory", 1000},
				{"Bug", 10000},
			};

		public async Task<IEnumerable<int>> GetTargets(int accountId, MetricSetup metricSetup)
		{
			await Task.Delay(Constants.TargetReadingTime);

			var entityTypes = metricSetup.EntityTypes.Split(',').Select(x => x.Trim());
			var totalCount = 0;

			foreach (var entityType in entityTypes)
			{
				int entityCount;

				if (EntityRegistry.TryGetValue(entityType, out entityCount))
				{
					totalCount += entityCount;
				}
			}

			return Enumerable.Range(1, totalCount);
		}
	}

	internal class Partitioner
	{
		public IEnumerable<IEnumerable<int>> Create(IEnumerable<int> ids)
		{
			var buffer = new List<int>(Constants.PartitionSize);

			foreach (var id in ids)
			{
				buffer.Add(id);

				if (buffer.Count == buffer.Capacity)
				{
					yield return buffer;
					buffer = new List<int>(Constants.PartitionSize);
				}
			}

			if (buffer.Any())
			{
				yield return buffer;
			}
		}
	}
}
