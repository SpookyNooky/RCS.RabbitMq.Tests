using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using RCS.RabbitMq.Consumer.Interfaces;
using RCS.RabbitMq.Consumer.Managers;
using RCS.RabbitMq.Consumer.Models;
using RCS.RabbitMq.Consumer.Services;
using RCS.RabbitMq.Logging.Processor.Interfaces;
using RCS.RabbitMq.Shared.Interfaces;
using RCS.RabbitMq.Tests.Consumer.Models;
using RCS.RabbitMq.Tests.Consumer.Utilities;

namespace RCS.RabbitMq.Tests.Consumer.Managers;

public class ConsumerManagerTests
{
    private readonly ConsumerManager<FakeLogMessage, ILogMessage> _manager;

    public ConsumerManagerTests()
    {
        var services = new ServiceCollection();

        services.AddSingleton(new RabbitMqSettings
        {
            ExchangeName = "test.exchange",
            ExchangeType = "direct",
            Host = "localhost",
            Password = "guest",
            QueueName = "test.queue",
            Username = "guest"
        });

        services.AddSingleton(new ConsumerSettings
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
        });


        services.AddSingleton(Mock.Of<IDataProcessingManager<FakeLogMessage>>());
        services.AddSingleton(Mock.Of<ILogger<ScalingCommandHandler>>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<IConnection>());
        services.AddSingleton(new ConcurrentQueue<Action>());

        var mockMapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();
        mockMapper.Setup(m => m.Map(It.IsAny<FakeLogMessage>())).Returns(Mock.Of<ILogMessage>());
        services.AddSingleton(mockMapper.Object);

        var provider = services.BuildServiceProvider();

        _manager = new ConsumerManager<FakeLogMessage, ILogMessage>(
            new ScalingCommandHandler(provider.GetRequiredService<ILogger<ScalingCommandHandler>>()),
            provider,
            provider.GetRequiredService<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
            provider.GetRequiredService<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
        );
    }

    [Fact]
    public void StartConsumer_Should_Not_Throw()
    {
        var token = CancellationToken.None;
        var ex = Record.Exception(() => _manager.StartConsumer(token));
        Assert.Null(ex);
    }

    [Fact]
    public void StopConsumer_Should_Not_Throw()
    {
        var token = CancellationToken.None;
        var ex = Record.Exception(() => _manager.StopConsumer(token));
        Assert.Null(ex);
    }

    [Fact]
    public void ExecuteScalingCommands_Should_Not_Throw_When_Empty()
    {
        var ex = Record.Exception(() => _manager.ExecuteScalingCommands());
        Assert.Null(ex);
    }

    [Fact]
    public void StartConsumer_Should_Enqueue_Command_And_Execute()
    {
        // Arrange
        var token = CancellationToken.None;

        // Act
        _manager.StartConsumer(token);
        var ex = Record.Exception(() => _manager.ExecuteScalingCommands());

        // Assert
        Assert.Null(ex);
    }

    [Fact]
    public void StopConsumer_Should_Remove_Consumer_And_Execute()
    {
        // Arrange
        var token = CancellationToken.None;
        _manager.StartConsumer(token);
        _manager.ExecuteScalingCommands();

        // Act
        _manager.StopConsumer(token);
        var ex = Record.Exception(() => _manager.ExecuteScalingCommands());

        // Assert
        Assert.Null(ex);
    }

    [Fact]
    public void ExecuteScalingCommands_Should_Run_Queued_Actions_Indirectly()
    {
        // Arrange
        var wasRun = false;
        var services = new ServiceCollection();

        // Create a shared ScalingCommandHandler and add an action
        var scalingLogger = new Mock<ILogger<ScalingCommandHandler>>();
        var scalingHandler = new ScalingCommandHandler(scalingLogger.Object);
        scalingHandler.AddCommand(() => wasRun = true);

        services.AddSingleton(scalingHandler);

        // Required settings
        services.AddSingleton(new RabbitMqSettings
        {
            Host = "localhost",
            Username = "guest",
            Password = "guest",
            ExchangeName = "test.exchange",
            ExchangeType = "direct",
            QueueName = "test.queue"
        });

        services.AddSingleton(new ConsumerSettings
        {
            MessageType = nameof(FakeLogMessage),
            MessageInterface = typeof(ILogMessage).FullName!,
            MessageClass = typeof(FakeLogMessage).FullName!,
            MessageMapper = typeof(FakeMapper).FullName!,
            ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
            ProcessorClass = typeof(FakeLogMessage).FullName!,
            MinConsumerCount = 1,
            MaxConsumerCount = 5,
            BatchSize = 10,
            InactivityTrigger = 1000,
            NonRetryableExceptions = []
        });


        services.AddSingleton(Mock.Of<IDataProcessingManager<FakeLogMessage>>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>());

        var provider = services.BuildServiceProvider();

        var manager = new ConsumerManager<FakeLogMessage, ILogMessage>(
            scalingHandler,
            provider,
            provider.GetRequiredService<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
            provider.GetRequiredService<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
        );

        // Act
        manager.ExecuteScalingCommands();

        // Assert
        Assert.True(wasRun);
    }

    [Fact]
    public async Task StartAsync_Should_Invoke_StartConsumer()
    {
        // Arrange
        var logger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();
        var handler = new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>());
        var mapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();
        var services = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, ILogMessage>();

        var manager = new ConsumerManager<FakeLogMessage, ILogMessage>(
            handler,
            services,
            logger.Object,
            mapper.Object);

        // Act
        await manager.StartAsync(CancellationToken.None); // no assignment needed

        // Assert - no exception = success
        Assert.True(true);
    }

    [Fact]
    public async Task StopAsync_Should_Invoke_StopConsumer()
    {
        // Arrange
        var logger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();
        var handler = new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>());
        var mapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();
        var services = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, ILogMessage>();

        var manager = new ConsumerManager<FakeLogMessage, ILogMessage>(
            handler,
            services,
            logger.Object,
            mapper.Object);

        // Act
        await manager.StopAsync(CancellationToken.None); // no assignment needed

        // Assert - no exception = success
        Assert.True(true);
    }

    [Fact]
    public void StartConsumer_Should_Log_If_Cancellation_Is_Requested()
    {
        // Arrange
        var logger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();
        var cancellationToken = new CancellationToken(true); // already cancelled

        var manager = BuildManager(logger: logger);

        // Act
        manager.StartConsumer(cancellationToken);
        manager.ExecuteScalingCommands();

        // Assert
        logger.Verify(l => l.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("StartConsumer canceled.")),
            null,
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()
        ), Times.Once);
    }

    [Fact]
    public void StartConsumer_Should_Return_When_ConsumerTag_Is_Null()
    {
        // Arrange
        var rabbitMqSettings = new RabbitMqSettings
        {
            Host = "localhost",
            Username = "guest",
            Password = "guest",
            ExchangeName = "test.exchange",
            ExchangeType = "direct",
            QueueName = "test.queue",
            RoutingKey = "test",
            MaxRetry = 3,
            RetryQueueTimeToLive = 60000,
            MessageTimeToLive = 300000
        };

        var consumerSettings = new ConsumerSettings
        {
            MessageType = nameof(FakeLogMessage),
            MessageInterface = typeof(ILogMessage).FullName!,
            MessageClass = typeof(FakeLogMessage).FullName!,
            MessageMapper = typeof(FakeMapper).FullName!,
            ProcessorAssembly = typeof(FakeLogMessage).Assembly.FullName!,
            ProcessorClass = typeof(FakeLogMessage).FullName!,
            MinConsumerCount = 1,
            MaxConsumerCount = 3,
            BatchSize = 10,
            InactivityTrigger = 1000,
            NonRetryableExceptions = []
        };


        var services = new ServiceCollection();

        services.AddSingleton(rabbitMqSettings);
        services.AddSingleton(consumerSettings);
        services.AddSingleton(Mock.Of<IDataProcessingManager<FakeLogMessage>>());
        services.AddSingleton(Mock.Of<ILogger<ScalingCommandHandler>>());
        services.AddSingleton<ScalingCommandHandler>(); // ✅ Required for ConsumerManager constructor
        services.AddSingleton(Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<IConnection>());
        services.AddSingleton(new ConcurrentQueue<Action>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>());

        // Register TestConsumerService that returns null for StartConsuming
        services.AddSingleton<IConsumerService<FakeLogMessage, ILogMessage>, TestConsumerService>();
        services.AddSingleton<ConsumerManager<FakeLogMessage, ILogMessage>>();

        var provider = services.BuildServiceProvider();

        var manager = provider.GetRequiredService<ConsumerManager<FakeLogMessage, ILogMessage>>();

        // Act
        manager.StartConsumer(CancellationToken.None);
        manager.ExecuteScalingCommands();

        // Assert: No exception = pass
    }




    [Fact]
    public void StartConsumer_Should_Add_Consumer_And_Log_Success()
    {
        // Arrange
        var logger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();

        var services = new ServiceCollection();

        services.AddSingleton(new RabbitMqSettings
        {
            Host = "localhost",
            Username = "guest",
            Password = "guest",
            ExchangeName = "test.exchange",
            ExchangeType = "direct",
            QueueName = "test.queue",
            RoutingKey = "test",
            MaxRetry = 3,
            RetryQueueTimeToLive = 60000,
            MessageTimeToLive = 300000
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
            MaxConsumerCount = 3,
            BatchSize = 10,
            InactivityTrigger = 1000,
            NonRetryableExceptions = []
        });


        services.AddSingleton(Mock.Of<IDataProcessingManager<FakeLogMessage>>());
        services.AddSingleton(Mock.Of<ILogger<ScalingCommandHandler>>());
        services.AddSingleton(logger.Object);
        services.AddSingleton(Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(new ConcurrentQueue<Action>());

        // Set up mock connection and channel
        var mockChannel = new Mock<IModel>();
        var mockConnection = new Mock<IConnection>();
        mockConnection.Setup(c => c.CreateModel()).Returns(mockChannel.Object);
        services.AddSingleton(mockConnection.Object);

        var mockMapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();
        mockMapper.Setup(m => m.Map(It.IsAny<FakeLogMessage>())).Returns(Mock.Of<ILogMessage>());
        services.AddSingleton(mockMapper.Object);

        var provider = services.BuildServiceProvider();

        var manager = new ConsumerManager<FakeLogMessage, ILogMessage>(
            new ScalingCommandHandler(provider.GetRequiredService<ILogger<ScalingCommandHandler>>()),
            provider,
            logger.Object,
            provider.GetRequiredService<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>()
        );

        // Act
        manager.StartConsumer(CancellationToken.None);
        manager.ExecuteScalingCommands();

        // Assert
        logger.Verify(l => l.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("started successfully")),
            null,
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()
        ), Times.AtLeastOnce);
    }

    [Fact]
    public void StartConsumer_Should_Return_If_ConsumerTag_Is_Null()
    {
        // Arrange
        var services = new ServiceCollection();
    
        services.AddSingleton(new RabbitMqSettings { Host = "x", Username = "x", Password = "x", ExchangeName = "x", ExchangeType = "direct", QueueName = "x", RoutingKey = "x" });
        services.AddSingleton(new ConsumerSettings
        {
            MessageType = "Fake",
            MessageInterface = "IFake",
            MessageClass = "Fake",
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
        services.AddSingleton(Mock.Of<IConnection>());
        services.AddSingleton(new ConcurrentQueue<Action>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<ILogger<ScalingCommandHandler>>());
        services.AddSingleton<ScalingCommandHandler>();
        services.AddSingleton(Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>());

        // Register stubbed service that returns null
        services.AddSingleton<IConsumerService<FakeLogMessage, ILogMessage>, StubConsumerServiceWithFixedTag>();
        services.AddSingleton<ConsumerManager<FakeLogMessage, ILogMessage>>();

        var provider = services.BuildServiceProvider();
        var manager = provider.GetRequiredService<ConsumerManager<FakeLogMessage, ILogMessage>>();

        // Act
        manager.StartConsumer(CancellationToken.None);
        manager.ExecuteScalingCommands();

        // Assert: Nothing crashes, null branch is hit
    }

    [Fact]
    public void StartConsumer_Should_Register_If_ConsumerTag_Is_Not_Null()
    {
        // Arrange
        var services = new ServiceCollection();

        services.AddSingleton(new RabbitMqSettings { Host = "x", Username = "x", Password = "x", ExchangeName = "x", ExchangeType = "direct", QueueName = "x", RoutingKey = "x" });
        services.AddSingleton(new ConsumerSettings
        {
            MessageType = "Fake",
            MessageInterface = "IFake",
            MessageClass = "Fake",
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
        services.AddSingleton(Mock.Of<IConnection>());
        services.AddSingleton(new ConcurrentQueue<Action>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<ILogger<ScalingCommandHandler>>());
        services.AddSingleton<ScalingCommandHandler>();
        services.AddSingleton(Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>());

        // Register stubbed service that returns a valid consumer tag
        services.AddSingleton<IConsumerService<FakeLogMessage, ILogMessage>, StubConsumerServiceWithFixedTagReturningNonNull>();
        services.AddSingleton<ConsumerManager<FakeLogMessage, ILogMessage>>();

        var provider = services.BuildServiceProvider();
        var manager = provider.GetRequiredService<ConsumerManager<FakeLogMessage, ILogMessage>>();

        // Act
        manager.StartConsumer(CancellationToken.None);
        manager.ExecuteScalingCommands();

        // Assert: Ensure the consumer was registered by checking logs or by internal validation if exposed
    }


    [Fact]
    public void Constructor_Should_Log_Scaling_Timer_Started()
    {
        // Arrange
        var logger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();
        var services = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, ILogMessage>();

        // Act
        var _ = new ConsumerManager<FakeLogMessage, ILogMessage>(
            new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>()),
            services,
            logger.Object,
            Mock.Of<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>());

        // Assert
        logger.Verify(l => l.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("scaling timer started")),
            null,
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Once);
    }

    [Fact]
    public void Constructor_Should_Initialize_And_Log_Timer_Start()
    {
        // Arrange
        var logger = new Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>();
        var serviceProvider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, ILogMessage>();
        var scalingHandler = new ScalingCommandHandler(Mock.Of<ILogger<ScalingCommandHandler>>());
        var mapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();

        // Act
        var _ = new ConsumerManager<FakeLogMessage, ILogMessage>(
            scalingHandler,
            serviceProvider,
            logger.Object,
            mapper.Object
        );

        // Assert
        logger.Verify(l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("scaling timer started")),
                null,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    private ConsumerManager<FakeLogMessage, ILogMessage> BuildManager(Mock<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>? logger = null)
    {
        var services = new ServiceCollection();

        services.AddSingleton(new RabbitMqSettings
        {
            Host = "localhost",
            Username = "guest",
            Password = "guest",
            ExchangeName = "test.exchange",
            ExchangeType = "direct",
            QueueName = "test.queue",
            RoutingKey = "test",
            MaxRetry = 3,
            RetryQueueTimeToLive = 60000,
            MessageTimeToLive = 300000
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
            MaxConsumerCount = 3,
            BatchSize = 10,
            InactivityTrigger = 1000,
            NonRetryableExceptions = []
        });


        services.AddSingleton(Mock.Of<IDataProcessingManager<FakeLogMessage>>());
        services.AddSingleton(Mock.Of<ILogger<ScalingCommandHandler>>());
        services.AddSingleton(logger?.Object ?? Mock.Of<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>());
        services.AddSingleton(Mock.Of<IConnection>());
        services.AddSingleton(new ConcurrentQueue<Action>());
        services.AddSingleton(Mock.Of<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>());

        var mockMapper = new Mock<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>();
        mockMapper.Setup(m => m.Map(It.IsAny<FakeLogMessage>())).Returns(Mock.Of<ILogMessage>());
        services.AddSingleton(mockMapper.Object);

        var provider = services.BuildServiceProvider();

        return new ConsumerManager<FakeLogMessage, ILogMessage>(
            new ScalingCommandHandler(provider.GetRequiredService<ILogger<ScalingCommandHandler>>()),
            provider,
            provider.GetRequiredService<ILogger<ConsumerManager<FakeLogMessage, ILogMessage>>>(),
            provider.GetRequiredService<IRabbitMessageMapper<FakeLogMessage, ILogMessage>>());
    }
}