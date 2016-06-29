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

namespace Recalculate.Calculator
{
	internal static class Constants
	{
		public static readonly string RabbitMqHostName = "192.168.99.100";

		public static readonly ushort RabbitMqPrefetchCount = 4;

		public static readonly TimeSpan MaxMetricExecutionTime = TimeSpan.FromMilliseconds(800);
		public static readonly TimeSpan MetricSetupReadingTime = TimeSpan.FromMilliseconds(1);

		public static readonly string ShortQueueName = "short_queue";
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
		public string EventId { get; set; }

		[ProtoMember(5, IsRequired = true)]
		public string CommandId { get; set; }
	}

	#endregion

	internal class Program
	{
		static void Main()
		{
			using (Calculator.Run())
			{
				Console.WriteLine($" [{DateTime.Now.TimeOfDay.ToString(@"hh\:mm\:ss\.ff")}] Started App.");
				Console.ReadLine();
			}
		}
	}

	internal class Calculator : IDisposable
	{
		private readonly IConnection _connection;
		private readonly IModel _channel;
		private readonly Listener<CalculateMetricCommand> _listener;

		public static IDisposable Run()
		{
			return new Calculator();
		}

		private Calculator()
		{
			var factory = new ConnectionFactory { HostName = Constants.RabbitMqHostName };

			_connection = factory.CreateConnection();
			_channel = _connection.CreateModel();
			_channel.BasicQos(0, Constants.RabbitMqPrefetchCount, true);

			_listener = new Listener<CalculateMetricCommand>(
				Constants.ShortQueueName,
				new CalculateMetricCommandHandler());
			_listener.Start(_channel);
		}

		public void Dispose()
		{
			_listener.Dispose();
			_channel.Dispose();
			_connection.Dispose();
		}
	}

	internal interface IListener : IDisposable
	{
		void Start(IModel channel);
	}

	internal abstract class ListenerBase<TMessage> : IListener
	{
		private IModel _channel;
		private readonly string _queueName;
		private string _consumerTag;
		private CancellationTokenSource _cancellationTokenSource;

		protected ListenerBase(string queueName)
		{
			_queueName = queueName;
		}

		public void Start(IModel channel)
		{
			_channel = channel;
			_channel.QueueDeclare(
				queue: _queueName,
				durable: false,
				exclusive: false,
				autoDelete: false,
				arguments: null);

			_cancellationTokenSource = new CancellationTokenSource();

			var consumer = new EventingBasicConsumer(_channel);
			consumer.Received += ConsumerOnReceived;
			_consumerTag = _channel.BasicConsume(
				queue: _queueName,
				noAck: false,
				consumer: consumer);
		}

		private async void ConsumerOnReceived(object sender, BasicDeliverEventArgs eArgs)
		{
			try
			{
				await ConsumerOnReceived(eArgs, _cancellationTokenSource.Token);
			}
			catch (OperationCanceledException)
			{
			}
		}

		private async Task ConsumerOnReceived(BasicDeliverEventArgs eArgs, CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();

			var message = new ProtoBufSerializer().Deserialize<TMessage>(eArgs.Body);
			//Console.WriteLine($" [x] Received {JsonConvert.SerializeObject(message)} \n");

			var handled = true;

			cancellationToken.ThrowIfCancellationRequested();

			try
			{
				await HandleImpl(message, cancellationToken);
			}
			catch (Exception ex) when (!(ex is OperationCanceledException))
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

		protected abstract Task HandleImpl(TMessage message, CancellationToken cancellationToken);

		public void Dispose()
		{
			if (_consumerTag != null)
			{
				_channel?.BasicCancel(_consumerTag);
			}

			_cancellationTokenSource?.Cancel();
		}
	}

	internal class Listener<TMessage> : ListenerBase<TMessage>
	{
		private readonly IMessageHandler<TMessage> _handler;

		public Listener(string queueName, IMessageHandler<TMessage> handler) : base(queueName)
		{
			_handler = handler;
		}

		protected override async Task HandleImpl(TMessage message, CancellationToken cancellationToken)
		{
			await _handler.HandleAsync(message, cancellationToken);
		}
	}

	internal interface IMessageHandler<in TMessage>
	{
		Task HandleAsync(TMessage message, CancellationToken cancellationToken);
	}

	internal class CalculateMetricCommandHandler : IMessageHandler<CalculateMetricCommand>
	{
		public async Task HandleAsync(CalculateMetricCommand message, CancellationToken cancellationToken)
		{
			Console.WriteLine($" [{DateTime.Now.TimeOfDay.ToString(@"hh\:mm\:ss\.ff")}] Processing CalculateMetricCommand#{message.CommandId}");

			foreach (var targetId in message.TargetIds)
			{
				var actualMetricSetup = new MetricSetupService().Get(message.AccountId, message.MetricSetup.Id);
				var delay = new Random(DateTime.UtcNow.Millisecond).Next(Constants.MaxMetricExecutionTime.Milliseconds);
				await Task.Delay(delay, cancellationToken);
			}

			Console.WriteLine($" [{DateTime.Now.TimeOfDay.ToString(@"hh\:mm\:ss\.ff")}] Processed  CalculateMetricCommand#{message.CommandId}");
		}

		private static bool Equals(MetricSetup setup1, MetricSetup setup2) =>
			setup1.MetricId == setup2.MetricId && setup1.EntityTypes == setup2.EntityTypes;
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

	internal class MetricSetupService
	{
		public MetricSetup Get(int accountId, int id)
		{
			using (var reader = new StreamReader(File.OpenRead(@"metric_setups.json")))
			{
				Thread.Sleep(Constants.MetricSetupReadingTime);

				return JsonConvert.DeserializeAnonymousType(reader.ReadToEnd(),
					new[] { new { AccountId = default(int), Items = new MetricSetup[0] } })
					.Where(x => x.AccountId == accountId)
					.SelectMany(x => x.Items)
					.FirstOrDefault(x => x.Id == id);
			}
		}
	}
}
