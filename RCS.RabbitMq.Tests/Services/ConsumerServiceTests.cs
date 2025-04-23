using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RCS.RabbitMq.Consumer.Interfaces;
using RCS.RabbitMq.Consumer.Managers;
using RCS.RabbitMq.Consumer.Models;
using RCS.RabbitMq.Consumer.Services;
using RCS.RabbitMq.Logging.Processor.Interfaces;
using RCS.RabbitMq.Shared.Interfaces;
using RCS.RabbitMq.Tests.Consumer.Models;
using RCS.RabbitMq.Tests.Consumer.Utilities;

namespace RCS.RabbitMq.Tests.Consumer.Services;

public class ConsumerServiceTests
{
    [Fact]
    public void StartConsuming_Should_Start_Without_Exception()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var settings = new RabbitMqSettings
        {
            Host = "localhost",
            Username = "guest",
            Password = "guest",
            QueueName = "test.queue",
            ExchangeName = "test.exchange",
            ExchangeType = "direct"
        };

        var consumerSettings = new ConsumerSettings
        {
            BatchSize = 10,
            InactivityTrigger = 1000,
            MaxConsumerCount = 5,
            MinConsumerCount = 1,
            MessageClass = typeof(FakeLogMessage).FullName!,
            MessageInterface = typeof(ILogMessage).FullName!,
            MessageMapper = typeof(FakeMapper).FullName!,
            MessageType = nameof(FakeLogMessage),
            ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
            ProcessorClass = typeof(FakeLogMessage).FullName!,
            NonRetryableExceptions = []
        };

        var queue = new ConcurrentQueue<Action>();
        var mockLogger = new Mock<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>();
        var mockConnection = new Mock<IConnection>();
        var mockChannel = new Mock<IModel>();
        mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);

        var scalingHandler = new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>());
        var mockConsumerLogger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();
        var mockMapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();

        var consumerManager = new ConsumerManager<FakeLogMessage, ILogMessage>(
            scalingHandler,
            new ServiceCollection().BuildServiceProvider(),
            mockConsumerLogger.Object,
            mockMapper.Object
        );

        var service = new ConsumerService<FakeLogMessage, ILogMessage>(
            mockProcessor.Object,
            settings,
            consumerSettings,
            queue,
            mockLogger.Object,
            mockConnection.Object,
            consumerManager
        );

        // Act
        var tag = service.StartConsuming(CancellationToken.None);

        // Assert
        Assert.NotNull(tag);
    }

    [Fact]
    public void StopConsuming_Should_Stop_Without_Exception()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var settings = new RabbitMqSettings
        {
            Host = "localhost",
            Username = "guest",
            Password = "guest",
            QueueName = "test.queue",
            ExchangeName = "test.exchange",
            ExchangeType = "direct"
        };

        var consumerSettings = new ConsumerSettings
        {
            BatchSize = 10,
            InactivityTrigger = 1000,
            MaxConsumerCount = 5,
            MinConsumerCount = 1,
            MessageClass = typeof(FakeLogMessage).FullName!,
            MessageInterface = typeof(ILogMessage).FullName!,
            MessageMapper = typeof(FakeMapper).FullName!,
            MessageType = nameof(FakeLogMessage),
            ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
            ProcessorClass = typeof(FakeLogMessage).FullName!,
            NonRetryableExceptions = []
        };

        var queue = new ConcurrentQueue<Action>();
        var mockLogger = new Mock<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>();
        var mockConnection = new Mock<IConnection>();
        var mockChannel = new Mock<IModel>();
        mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);

        var scalingHandler = new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>());
        var mockConsumerLogger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();
        var mockMapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();

        var consumerManager = new ConsumerManager<FakeLogMessage, ILogMessage>(
            scalingHandler,
            new ServiceCollection().BuildServiceProvider(),
            mockConsumerLogger.Object,
            mockMapper.Object
        );

        var service = new ConsumerService<FakeLogMessage, ILogMessage>(
            mockProcessor.Object,
            settings,
            consumerSettings,
            queue,
            mockLogger.Object,
            mockConnection.Object,
            consumerManager
        );

        service.StartConsuming(CancellationToken.None);

        // Act
        service.StopConsuming(CancellationToken.None);

        // Assert
        // Nothing to assert yet — if no exceptions are thrown, it's a pass
        Assert.True(true);
    }

    [Fact]
    public async Task HandleMessageAsync_Should_Process_Message()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var message = new FakeLogMessage();

        var mockLogger = new Mock<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>();
        var mockConnection = new Mock<IConnection>();
        var mockChannel = new Mock<IModel>();
        mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);

        var consumerManager = new ConsumerManager<FakeLogMessage, ILogMessage>(
            new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
            new ServiceCollection().BuildServiceProvider(),
            Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
        );

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue"
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            mockLogger.Object,
            mockConnection.Object,
            consumerManager
        );

        var cancellationToken = CancellationToken.None;

        // Act
        await service.InvokeHandleMessageAsync(message, cancellationToken);

        // Assert
        mockProcessor.Verify(p => p.ProcessBatchAsync(It.Is<IEnumerable<FakeLogMessage>>(msgs => msgs.Contains(message))), Times.Once);
    }

    [Fact]
    public async Task ProcessBatchAsync_Should_Call_DataProcessingManager()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var messages = new List<FakeLogMessage> { new() };
        mockProcessor.Setup(p => p.ProcessBatchAsync(messages)).ReturnsAsync(true);

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue"
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        // Act
        var result = await service.InvokeProcessBatchAsync(messages);

        // Assert
        Assert.True(result);
        mockProcessor.Verify(p => p.ProcessBatchAsync(messages), Times.Once);
    }

    [Fact]
    public async Task PrepareBatchAsync_Should_Ack_On_Success()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var message = new FakeLogMessage { DeliveryTag = 1 };

        mockProcessor.Setup(p => p.ProcessBatchAsync(It.IsAny<IEnumerable<FakeLogMessage>>()))
                     .ReturnsAsync(true);

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings { Host = "localhost", Username = "guest", Password = "guest", ExchangeName = "test.exchange", ExchangeType = "direct", QueueName = "test.queue", RoutingKey = "test.key" },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var queue = new ConcurrentQueue<FakeLogMessage>();
        queue.Enqueue(message);

        var deliveryTags = new ConcurrentDictionary<ulong, byte>();
        deliveryTags.TryAdd(1, 0);

        // Act
        await service.InvokePrepareBatchAsync(queue, deliveryTags, mockChannel.Object);

        // Assert
        mockChannel.Verify(c => c.BasicAck(1, true), Times.Once);
    }

    [Fact]
    public async Task PrepareBatchAsync_Should_Nack_On_Exception()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var message = new FakeLogMessage { DeliveryTag = 2 };

        mockProcessor.Setup(p => p.ProcessBatchAsync(It.IsAny<IEnumerable<FakeLogMessage>>()))
                     .ThrowsAsync(new Exception("Simulated failure"));

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings { Host = "localhost", Username = "guest", Password = "guest", ExchangeName = "test.exchange", ExchangeType = "direct", QueueName = "test.queue", RoutingKey = "test.key" },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var queue = new ConcurrentQueue<FakeLogMessage>();
        queue.Enqueue(message);

        var deliveryTags = new ConcurrentDictionary<ulong, byte>();
        deliveryTags.TryAdd(2, 0);

        // Act
        await service.InvokePrepareBatchAsync(queue, deliveryTags, mockChannel.Object);

        // Assert
        mockChannel.Verify(c => c.BasicNack(2, false, false), Times.Once);
    }

    [Fact]
    public async Task RetryOrDeadLetter_Should_Retry_If_Under_MaxRetry()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var message = new FakeLogMessage
        {
            DeliveryTag = 1,
            Headers = new Dictionary<string, object>
            {
                ["x-death"] = new List<object>
            {
                new Dictionary<string, object>
                {
                    ["count"] = (long)2
                }
            }
            }
        };

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key",
                MaxRetry = 5
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        // Act
        var method = typeof(ConsumerService<FakeLogMessage, ILogMessage>)
            .GetMethod("RetryOrDeadLetter", BindingFlags.NonPublic | BindingFlags.Instance);

        var task = (Task?)method!.Invoke(service, [message, mockChannel.Object]);
        if (task != null)
            await task;

        // Assert
        mockChannel.Verify(c => c.BasicAck(1, false), Times.Once);
    }

    [Fact]
    public async Task MoveToDeadLetterQueue_Should_Publish_And_Ack_Message()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();

        var message = new FakeLogMessage { DeliveryTag = 10 };

        var mockProps = new Mock<IBasicProperties>();
        mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key"
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var method = typeof(ConsumerService<FakeLogMessage, ILogMessage>)
            .GetMethod("MoveToDeadLetterQueue", BindingFlags.NonPublic | BindingFlags.Instance);

        // Act
        var task = (Task?)method!.Invoke(service, [message, mockChannel.Object]);
        if (task != null)
            await task;

        // Assert
        mockChannel.Verify(c => c.BasicAck(10, false), Times.Once);

        Assert.Contains(mockChannel.Invocations, invocation =>
            invocation.Method.Name == "BasicPublish" &&
            invocation.Arguments[0].Equals("test.exchange") &&
            invocation.Arguments[1].Equals("test.key_dead")
        );
    }

    [Fact]
    public async Task RequeueMessage_Should_Ack_And_Publish_To_Retry_Queue()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var message = new FakeLogMessage
        {
            DeliveryTag = 20,
            Headers = new Dictionary<string, object> { { "test", "value" } }
        };

        var mockProps = new Mock<IBasicProperties>();
        mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key"
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var method = typeof(ConsumerService<FakeLogMessage, ILogMessage>)
            .GetMethod("RequeueMessage", BindingFlags.NonPublic | BindingFlags.Instance);

        // Act
        var task = (Task?)method!.Invoke(service, [message, mockChannel.Object]);
        if (task != null)
            await task;

        // Assert
        mockChannel.Verify(c => c.BasicAck(20, false), Times.Once);

        Assert.Contains(mockChannel.Invocations, invocation =>
            invocation.Method.Name == "BasicPublish" &&
            invocation.Arguments[0].Equals("test.exchange") &&
            invocation.Arguments[1].Equals("test.key_retry")
        );
    }

    [Fact]
    public async Task HandleError_Should_Invoke_RetryOrDeadLetter_For_TimeoutException()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var mockProps = new Mock<IBasicProperties>();

        var publishCalled = false;

        var message = new FakeLogMessage
        {
            DeliveryTag = 99,
            Headers = new Dictionary<string, object>
            {
                ["x-death"] = new List<object>
            {
                new Dictionary<string, object>
                {
                    ["count"] = (long)1
                }
            }
            }
        };

        mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);

        // Use Callback ONLY — do NOT use Setup for extension methods
        mockChannel
            .As<IModel>()
            .Object
            .BasicPublish("test.exchange", "test.key_retry", mockProps.Object, ReadOnlyMemory<byte>.Empty); // no-op to satisfy Moq

        // Use side effect to capture if BasicPublish is *called*
        var spyChannel = new SpyChannel(() => publishCalled = true);

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key",
                MaxRetry = 3
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var method = typeof(ConsumerService<FakeLogMessage, ILogMessage>)
            .GetMethod("HandleError", BindingFlags.NonPublic | BindingFlags.Instance);

        var exception = new TimeoutException("Simulated timeout");

        // Act
        var task = (Task?)method!.Invoke(service, [message, exception, spyChannel]);
        if (task != null)
            await task;

        // Assert
        Assert.True(spyChannel.AckCalled, "Expected BasicAck to be called.");
        Assert.True(publishCalled, "Expected BasicPublish to be called.");
    }

    [Fact]
    public async Task ConsumerService_EndToEnd_Should_Process_And_Ack_Message()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var mockLogger = new Mock<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>();

        var message = new FakeLogMessage
        {
            DeliveryTag = 42,
            Headers = new Dictionary<string, object>()
        };

        mockProcessor
            .Setup(p => p.ProcessBatchAsync(It.IsAny<IEnumerable<FakeLogMessage>>()))
            .ReturnsAsync(true);

        var acked = false;
        mockChannel.Setup(c => c.BasicAck(42, true)).Callback(() => acked = true);
        mockChannel.Setup(c => c.CreateBasicProperties()).Returns(new Mock<IBasicProperties>().Object);

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key"
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            mockLogger.Object,
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var queue = new ConcurrentQueue<FakeLogMessage>();
        queue.Enqueue(message);

        var deliveryTags = new ConcurrentDictionary<ulong, byte>();
        deliveryTags.TryAdd(42, 0);

        // Act
        await service.InvokePrepareBatchAsync(queue, deliveryTags, mockChannel.Object);

        // Assert
        mockProcessor.Verify(p => p.ProcessBatchAsync(It.Is<IEnumerable<FakeLogMessage>>(m => m.Contains(message))), Times.Once);
        Assert.True(acked, "Expected BasicAck to be called.");
    }

    [Fact]
    public async Task ConsumerService_EndToEnd_Should_Nack_On_Process_Failure()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var message = new FakeLogMessage { DeliveryTag = 43 };

        mockProcessor
            .Setup(p => p.ProcessBatchAsync(It.IsAny<IEnumerable<FakeLogMessage>>()))
            .ThrowsAsync(new Exception("Simulated failure"));

        mockChannel.Setup(c => c.BasicNack(43, false, false)).Verifiable();

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key"
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var queue = new ConcurrentQueue<FakeLogMessage>();
        queue.Enqueue(message);

        var deliveryTags = new ConcurrentDictionary<ulong, byte>();
        deliveryTags.TryAdd(43, 0);

        // Act
        await service.InvokePrepareBatchAsync(queue, deliveryTags, mockChannel.Object);

        // Assert
        mockChannel.Verify(c => c.BasicNack(43, false, false), Times.Once);
    }

    [Fact]
    public async Task ConsumerService_Should_DeadLetter_When_Base_Exception_Is_Configured_As_NonRetryable()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var mockProps = new Mock<IBasicProperties>();

        var message = new FakeLogMessage { DeliveryTag = 99 };

        mockProcessor
            .Setup(p => p.ProcessBatchAsync(It.IsAny<IEnumerable<FakeLogMessage>>()))
            .ThrowsAsync(new Exception("Non-retryable error"));

        mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);
        mockChannel.Setup(c => c.BasicAck(99, false)).Verifiable();
        mockChannel.Setup(c => c.BasicPublish(
            "test.exchange",
            "test.key_dead",
            false,
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()
        )).Verifiable();

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key"
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = [typeof(Exception).FullName!]
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var queue = new ConcurrentQueue<FakeLogMessage>();
        queue.Enqueue(message);

        var deliveryTags = new ConcurrentDictionary<ulong, byte>();
        deliveryTags.TryAdd(99, 0);

        // Act
        await service.InvokePrepareBatchAsync(queue, deliveryTags, mockChannel.Object);

        // Assert
        mockChannel.Verify(c => c.BasicAck(99, false), Times.Once);
        mockChannel.Verify(c => c.BasicPublish(
            "test.exchange",
            "test.key_dead",
            false,
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()
        ), Times.Once);
    }

    [Fact]
    public async Task ConsumerService_EndToEnd_Should_Requeue_On_Retryable_Exception()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var mockProps = new Mock<IBasicProperties>();

        var message = new FakeLogMessage
        {
            DeliveryTag = 77,
            Headers = new Dictionary<string, object>
            {
                ["x-death"] = new List<object>
            {
                new Dictionary<string, object>
                {
                    ["count"] = (long)1
                }
            }
            }
        };

        mockProcessor
            .Setup(p => p.ProcessBatchAsync(It.IsAny<IEnumerable<FakeLogMessage>>()))
            .ThrowsAsync(new TimeoutException("Retryable failure"));

        mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);
        mockChannel.Setup(c => c.BasicAck(77, false)).Verifiable();
        mockChannel.Setup(c => c.BasicPublish(
            "test.exchange",
            "test.key_retry",
            false,
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()
        )).Verifiable();

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key",
                MaxRetry = 3
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var queue = new ConcurrentQueue<FakeLogMessage>();
        queue.Enqueue(message);

        var deliveryTags = new ConcurrentDictionary<ulong, byte>();
        deliveryTags.TryAdd(77, 0);

        // Act
        await service.InvokePrepareBatchAsync(queue, deliveryTags, mockChannel.Object);

        // Assert
        mockChannel.Verify(c => c.BasicAck(77, false), Times.Once);
        mockChannel.Verify(c => c.BasicPublish(
            "test.exchange",
            "test.key_retry",
            false,
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()
        ), Times.Once);
    }

    [Fact]
    public async Task ConsumerService_EndToEnd_Should_DeadLetter_When_Retry_Limit_Exceeded()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var mockProps = new Mock<IBasicProperties>();

        var message = new FakeLogMessage
        {
            DeliveryTag = 88,
            Headers = new Dictionary<string, object>
            {
                ["x-death"] = new List<object>
            {
                new Dictionary<string, object>
                {
                    ["count"] = (long)5
                }
            }
            }
        };

        mockProcessor
            .Setup(p => p.ProcessBatchAsync(It.IsAny<IEnumerable<FakeLogMessage>>()))
            .ThrowsAsync(new TimeoutException("Simulated retryable failure"));

        mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);
        mockChannel.Setup(c => c.BasicAck(88, false)).Verifiable();
        mockChannel.Setup(c => c.BasicPublish(
            "test.exchange",
            "test.key_dead",
            false,
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()
        )).Verifiable();

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key",
                MaxRetry = 3
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = []
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var queue = new ConcurrentQueue<FakeLogMessage>();
        queue.Enqueue(message);

        var deliveryTags = new ConcurrentDictionary<ulong, byte>();
        deliveryTags.TryAdd(88, 0);

        // Act
        await service.InvokePrepareBatchAsync(queue, deliveryTags, mockChannel.Object);

        // Assert
        mockChannel.Verify(c => c.BasicAck(88, false), Times.Once);
        mockChannel.Verify(c => c.BasicPublish("test.exchange", "test.key_dead", false,
            It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
    }

    [Fact]
    public async Task ConsumerService_Should_DeadLetter_When_Explicit_NonRetryable_Exception_Is_Thrown()
    {
        // Arrange
        var mockProcessor = new Mock<IDataProcessingManager<FakeLogMessage>>();
        var mockChannel = new Mock<IModel>();
        var mockProps = new Mock<IBasicProperties>();

        var message = new FakeLogMessage
        {
            DeliveryTag = 99,
            Headers = new Dictionary<string, object>() // retry count doesn't matter here
        };

        mockProcessor
            .Setup(p => p.ProcessBatchAsync(It.IsAny<IEnumerable<FakeLogMessage>>()))
            .ThrowsAsync(new InvalidOperationException("Non-retryable"));

        mockChannel.Setup(c => c.CreateBasicProperties()).Returns(mockProps.Object);
        mockChannel.Setup(c => c.BasicAck(99, false)).Verifiable();
        mockChannel.Setup(c => c.BasicPublish(
            "test.exchange",
            "test.key_dead",
            false,
            It.IsAny<IBasicProperties>(),
            It.IsAny<ReadOnlyMemory<byte>>()
        )).Verifiable();

        var service = new TestableConsumerService(
            mockProcessor.Object,
            new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                QueueName = "test.queue",
                RoutingKey = "test.key",
                MaxRetry = 3
            },
            new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(FakeLogMessage).FullName!,
                MessageInterface = typeof(ILogMessage).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = nameof(FakeLogMessage),
                ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
                ProcessorClass = typeof(FakeLogMessage).FullName!,
                NonRetryableExceptions = [typeof(InvalidOperationException).FullName!]
            },
            new ConcurrentQueue<Action>(),
            Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            Mock.Of<IConnection>(),
            new ConsumerManager<FakeLogMessage, ILogMessage>(
                new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
                new ServiceCollection().BuildServiceProvider(),
                Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
                Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
            )
        );

        var queue = new ConcurrentQueue<FakeLogMessage>();
        queue.Enqueue(message);

        var deliveryTags = new ConcurrentDictionary<ulong, byte>();
        deliveryTags.TryAdd(99, 0);

        // Act
        await service.InvokePrepareBatchAsync(queue, deliveryTags, mockChannel.Object);

        // Assert
        mockChannel.Verify(c => c.BasicAck(99, false), Times.Once);
        mockChannel.Verify(c => c.BasicPublish("test.exchange", "test.key_dead", false,
            It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
    }

    [Fact]
    public void StartConsumer_Should_Log_Warning_If_Already_Registered()
    {
        // Arrange
        var logger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();

        var services = new ServiceCollection();

        services.AddSingleton(new RabbitMqSettings
        {
            Host = "x",
            Username = "x",
            Password = "x",
            ExchangeName = "x",
            ExchangeType = "direct",
            QueueName = "x",
            RoutingKey = "x"
        });

        services.AddSingleton(new ConsumerSettings
        {
            MessageType = "FakeLogMessage",
            MessageInterface = "ILogMessage",
            MessageClass = "FakeLogMessage",
            MessageMapper = "FakeMapper",
            ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
            ProcessorClass = typeof(FakeLogMessage).FullName!,
            MinConsumerCount = 1,
            MaxConsumerCount = 1,
            BatchSize = 1,
            InactivityTrigger = 1,
            NonRetryableExceptions = []
        });

        services.AddSingleton(Mock.Of<IDataProcessingManager<FakeLogMessage>>());
        services.AddSingleton(new ConcurrentQueue<Action>());
        services.AddSingleton(Mock.Of<ILogger<ScalingCommandHandler>>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(logger.Object);
        services.AddSingleton<ScalingCommandHandler>();

        var mockMapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();
        mockMapper.Setup(m => m.Map(It.IsAny<FakeLogMessage>())).Returns(Mock.Of<ILogMessage>());
        services.AddSingleton(mockMapper.Object);

        // Setup mocked IModel/IConnection to avoid null ref
        var mockModel = new Mock<IModel>();
        var mockConnection = new Mock<IConnection>();
        mockConnection.Setup(c => c.CreateModel()).Returns(mockModel.Object);
        services.AddSingleton(mockConnection.Object);

        // ✅ Register the real ConsumerManager so it can be resolved during stub service creation
        services.AddSingleton<ConsumerManager<FakeLogMessage, ILogMessage>>();

        var provider = services.BuildServiceProvider();

        // Use fixed tag and controlled ConsumerService
        var fixedTag = "fixed-tag";
        var stubService = new StubConsumerServiceWithFixedTag(provider, fixedTag);

        var testManager = new TestableConsumerManager(
            provider.GetRequiredService<ScalingCommandHandler>(),
            provider,
            logger.Object,
            provider.GetRequiredService<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>(),
            stubService
        );

        // Pre-populate _activeConsumers with same tag
        var consumerDict = new ConcurrentDictionary<string, ConsumerService<FakeLogMessage, ILogMessage>>();
        consumerDict.TryAdd(fixedTag, stubService);
        typeof(ConsumerManager<FakeLogMessage, ILogMessage>)
            .GetField("_activeConsumers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(testManager, consumerDict);

        // Act
        testManager.StartConsumer(CancellationToken.None);
        testManager.ExecuteScalingCommands();

        // Assert
        logger.Verify(l => l.Log(
            LogLevel.Warning,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("already registered")),
            null,
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Once);
    }

    private class SpyChannel(Action onPublish) : IModel
    {
        public bool AckCalled { get; private set; }

        public IBasicProperties CreateBasicProperties() => new Mock<IBasicProperties>().Object;

        public void BasicAck(ulong deliveryTag, bool multiple)
        {
            AckCalled = true;
        }

        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties, ReadOnlyMemory<byte> body)
        {
            onPublish();
        }

        // --- Unused, stubbed or dummy returns for interface compliance ---
        public void Dispose() { }
        public void Abort() { }
        public void Abort(ushort replyCode, string replyText) { }
        public void BasicCancel(string consumerTag) { }
        public void BasicCancelNoWait(string consumerTag) { }
        public string BasicConsume(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive, IDictionary<string, object> arguments, IBasicConsumer consumer) => "";
        public BasicGetResult BasicGet(string queue, bool autoAck) => null!;
        public void BasicNack(ulong deliveryTag, bool multiple, bool requeue) { }
        public void BasicQos(uint prefetchSize, ushort prefetchCount, bool global) { }
        public void BasicRecover(bool requeue) { }
        public void BasicRecoverAsync(bool requeue) { }
        public void BasicReject(ulong deliveryTag, bool requeue) { }
        public void Close() { }
        public void Close(ushort replyCode, string replyText) { }
        public void ConfirmSelect() { }
        public IBasicPublishBatch CreateBasicPublishBatch() => null!;
        public void ExchangeBind(string destination, string source, string routingKey, IDictionary<string, object> arguments) { }
        public void ExchangeBindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments) { }
        public void ExchangeDeclare(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments) { }
        public void ExchangeDeclareNoWait(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments) { }
        public void ExchangeDeclarePassive(string exchange) { }
        public void ExchangeDelete(string exchange, bool ifUnused) { }
        public void ExchangeDeleteNoWait(string exchange, bool ifUnused) { }
        public void ExchangeUnbind(string destination, string source, string routingKey, IDictionary<string, object> arguments) { }
        public void ExchangeUnbindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments) { }
        public void QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) { }
        public void QueueBindNoWait(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) { }
        public QueueDeclareOk QueueDeclare(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments) => null!;
        public void QueueDeclareNoWait(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments) { }
        public QueueDeclareOk QueueDeclarePassive(string queue) => null!;
        public uint MessageCount(string queue) => 0;
        public uint ConsumerCount(string queue) => 0;
        public uint QueueDelete(string queue, bool ifUnused, bool ifEmpty) => 0;
        public void QueueDeleteNoWait(string queue, bool ifUnused, bool ifEmpty) { }
        public uint QueuePurge(string queue) => 0;
        public void QueueUnbind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) { }
        public void TxCommit() { }
        public void TxRollback() { }
        public void TxSelect() { }
        public bool WaitForConfirms() => true;
        public bool WaitForConfirms(TimeSpan timeout) => true;
        public bool WaitForConfirms(TimeSpan timeout, out bool timedOut) { timedOut = false; return true; }
        public void WaitForConfirmsOrDie() { }
        public void WaitForConfirmsOrDie(TimeSpan timeout) { }

        public int ChannelNumber => 1;
        public ShutdownEventArgs CloseReason => null!;
        public IBasicConsumer DefaultConsumer { get; set; } = null!;
        public string CurrentQueue => "";
        public TimeSpan ContinuationTimeout { get; set; }
        public bool IsClosed => false;
        public bool IsOpen => true;
        public ulong NextPublishSeqNo => 0;

        public event EventHandler<BasicAckEventArgs>? BasicAcks;
        public event EventHandler<BasicNackEventArgs>? BasicNacks;
        public event EventHandler<EventArgs>? BasicRecoverOk;
        public event EventHandler<BasicReturnEventArgs>? BasicReturn;
        public event EventHandler<CallbackExceptionEventArgs>? CallbackException;
        public event EventHandler<FlowControlEventArgs>? FlowControl;
        public event EventHandler<ShutdownEventArgs>? ModelShutdown;
    }

    private class TestableConsumerService(IDataProcessingManager<FakeLogMessage> processor, RabbitMqSettings settings, ConsumerSettings consumerSettings, ConcurrentQueue<Action> queue, ILogger<ConsumerService<FakeLogMessage, ILogMessage>> logger, IConnection connection, ConsumerManager<FakeLogMessage, ILogMessage> manager) : ConsumerService<FakeLogMessage, ILogMessage>(processor, settings, consumerSettings, queue, logger, connection, manager)
    {
        public Task InvokeHandleMessageAsync(FakeLogMessage message, CancellationToken token)
            => HandleMessageAsync(message, token);

        public Task<bool> InvokeProcessBatchAsync(IEnumerable<FakeLogMessage> messages)
            => ProcessBatchAsync(messages);

        public async Task InvokePrepareBatchAsync(
            ConcurrentQueue<FakeLogMessage> queue,
            ConcurrentDictionary<ulong, byte> deliveryTags,
            IModel channel)
        {
            var method = typeof(ConsumerService<FakeLogMessage, ILogMessage>)
                .GetMethod("PrepareBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance);

            if (method == null)
                throw new InvalidOperationException("PrepareBatchAsync method not found.");

            var task = (Task?)method.Invoke(this, [queue, deliveryTags, channel]);
            if (task != null)
                await task;
        }
    }
}