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
using RCS.RabbitMQ.WhatsApp.ActionLog.Processor.Interfaces;

namespace RCS.RabbitMq.Tests.Consumer.Utilities;

public static class TestHelper
{
    public static ServiceProvider BuildValidLogMessageServiceProvider(
        bool includeLogger = true,
        bool includeConsumerManager = true,
        bool includeRabbitMqSettings = true)
    {
        return BuildValidServiceProvider<FakeLogMessage, ILogMessage>(
            includeLogger,
            includeConsumerManager,
            includeRabbitMqSettings);
    }

    public static ServiceProvider BuildValidWhatsAppMessageServiceProvider(
        bool includeLogger = true,
        bool includeConsumerManager = true,
        bool includeRabbitMqSettings = true)
    {
        return BuildValidServiceProvider<FakeWhatsAppMessage, IWhatsAppMessage>(
            includeLogger,
            includeConsumerManager,
            includeRabbitMqSettings);
    }

    public static ServiceProvider BuildValidServiceProvider<TMessage, TContract>(
        bool includeLogger = true,
        bool includeConsumerManager = true,
        bool includeRabbitMqSettings = true)
        where TMessage : class, IAcknowledgeableRabbitMessage, new()
        where TContract : class, IBasicMessageContract
    {
        var services = new ServiceCollection();

        if (includeConsumerManager)
        {
            // Register required types for ConsumerManager
            services.AddSingleton<IConsumerManager<TMessage, TContract>, ConsumerManager<TMessage, TContract>>();
            services.AddSingleton<ConsumerManager<TMessage, TContract>>();
            services.AddSingleton<ScalingCommandHandler>();
            services.AddSingleton(new ConcurrentQueue<Action>());

            var mockConsumerManagerLogger = new Mock<ILogger<ConsumerManager<TMessage, TContract>>>();
            services.AddSingleton(mockConsumerManagerLogger.Object);

            var mockScalingLogger = new Mock<ILogger<ScalingCommandHandler>>();
            services.AddSingleton(mockScalingLogger.Object);

            var mockMapper = new Mock<IRabbitMessageMapper<TMessage, TContract>>();
            mockMapper.Setup(m => m.Map(It.IsAny<TMessage>())).Returns(Mock.Of<TContract>());
            services.AddSingleton(mockMapper.Object);

            var mockDataProcessor = new Mock<IDataProcessingManager<TMessage>>();
            services.AddSingleton(mockDataProcessor.Object);

            services.AddSingleton(new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(TMessage).FullName!,
                MessageInterface = typeof(TContract).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = typeof(TMessage).Name,
                ProcessorAssembly = typeof(TMessage).Assembly.FullName!,
                ProcessorClass = typeof(TMessage).FullName!,
                NonRetryableExceptions = []
            });
        }

        if (includeLogger)
        {
            var mockLogger = new Mock<ILogger<MonitoringService<TMessage, TContract>>>();
            services.AddSingleton(mockLogger.Object);
        }

        if (includeRabbitMqSettings)
        {
            services.AddSingleton(new RabbitMqSettings
            {
                ExchangeName = "test.exchange",
                ExchangeType = "fanout",
                Host = "localhost",
                Password = "guest",
                QueueName = "test.queue",
                Username = "guest"
            });
        }

        return services.BuildServiceProvider();
    }

    public static IServiceProvider BuildValidConsumerServiceProvider<TMessage, TContract>(
        bool includeLogger = true,
        bool includeConsumerManager = true,
        bool includeDataProcessor = true,
        bool includeRabbitMqSettings = true,
        bool includeConsumerSettings = true,
        bool includeScalingQueue = true,
        bool includeConnection = true,
        bool includeMapper = true)
        where TMessage : class, IAcknowledgeableRabbitMessage, new()
        where TContract : class, IBasicMessageContract
    {
        var services = new ServiceCollection();

        if (includeConsumerManager)
        {
            services.AddSingleton(Mock.Of<ILogger<ScalingCommandHandler>>());
            services.AddSingleton(Mock.Of<ILogger<ConsumerManager<TMessage, TContract>>>());
            services.AddSingleton<ScalingCommandHandler>();
            services.AddSingleton<ConsumerManager<TMessage, TContract>>();
        }

        if (includeDataProcessor)
        {
            var mock = new Mock<IDataProcessingManager<TMessage>>();
            services.AddSingleton(mock.Object);
        }

        if (includeLogger)
        {
            var mockLogger = new Mock<ILogger<ConsumerService<TMessage, TContract>>>();
            services.AddSingleton(mockLogger.Object);
        }

        if (includeRabbitMqSettings)
        {
            services.AddSingleton(new RabbitMqSettings
            {
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                Host = "localhost",
                Password = "guest",
                QueueName = "test.queue",
                Username = "guest"
            });
        }

        if (includeConsumerSettings)
        {
            services.AddSingleton(new ConsumerSettings
            {
                BatchSize = 10,
                InactivityTrigger = 1000,
                MaxConsumerCount = 5,
                MinConsumerCount = 1,
                MessageClass = typeof(TMessage).FullName!,
                MessageInterface = typeof(TContract).FullName!,
                MessageMapper = typeof(FakeMapper).FullName!,
                MessageType = typeof(TMessage).Name,
                ProcessorAssembly = typeof(TMessage).Assembly.FullName!,
                ProcessorClass = typeof(TMessage).FullName!,
                NonRetryableExceptions = []
            });
        }

        if (includeScalingQueue)
        {
            services.AddSingleton(new ConcurrentQueue<Action>());
        }

        if (includeConnection)
        {
            services.AddSingleton(Mock.Of<IConnection>());
        }

        if (includeMapper)
        {
            var mock = new Mock<IRabbitMessageMapper<TMessage, TContract>>();
            mock.Setup(m => m.Map(It.IsAny<TMessage>())).Returns(Mock.Of<TContract>());
            services.AddSingleton(mock.Object);
        }

        return services.BuildServiceProvider();
    }
}
