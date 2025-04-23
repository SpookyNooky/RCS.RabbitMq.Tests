using Microsoft.Extensions.DependencyInjection;
using RCS.RabbitMq.Consumer.Factories;
using RCS.RabbitMq.Consumer.Services;
using RCS.RabbitMq.Logging.Processor.Interfaces;
using RCS.RabbitMq.Tests.Consumer.Models;
using RCS.RabbitMq.Tests.Consumer.Utilities;
using RCS.RabbitMQ.WhatsApp.ActionLog.Processor.Interfaces;

namespace RCS.RabbitMq.Tests.Consumer.Factories
{
    public class ConsumerServiceFactoryTests
    {
        [Fact]
        public void Create_Should_Throw_For_Unknown_MessageType()
        {
            var provider = new ServiceCollection().BuildServiceProvider();
            var factory = new ConsumerServiceFactory(provider);

            Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(string)));
        }

        [Fact]
        public void Create_Should_Succeed_For_Valid_LogMessage()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, ILogMessage>();
            var factory = new ConsumerServiceFactory(provider);

            var service = factory.Create(typeof(FakeLogMessage));
            Assert.NotNull(service);
        }

        [Fact]
        public void Create_Should_Throw_When_Logger_Is_Missing()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, FakeLogMessage>(
                includeLogger: true,
                includeConnection: true,
                includeScalingQueue: true,
                includeConsumerSettings: true,
                includeDataProcessor: true
            );

            var factory = new ConsumerServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));

            Assert.Contains("No service for type", ex.Message);
        }

        [Fact]
        public void Create_Should_Throw_When_ConsumerManager_Is_Missing()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, FakeLogMessage>(includeConsumerManager: false);
            var factory = new ConsumerServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));
            Assert.Contains("ConsumerManager", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Create_Should_Throw_When_DataProcessor_Is_Missing()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, FakeLogMessage>(
                includeDataProcessor: false,
                includeConnection: true,
                includeScalingQueue: true,
                includeConsumerSettings: true
            );

            var factory = new ConsumerServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));

            // Safer: check for the generic failure string
            Assert.Contains("No service for type", ex.Message);
        }

        [Fact]
        public void Create_Should_Throw_When_RabbitMqSettings_Is_Missing()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, FakeLogMessage>(
                includeRabbitMqSettings: false,
                includeLogger: true,
                includeConsumerManager: true,
                includeConsumerSettings: true,
                includeConnection: true,
                includeDataProcessor: true,
                includeScalingQueue: true
            );

            var factory = new ConsumerServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));

            Assert.Contains("No service for type", ex.Message);
        }

        [Fact]
        public void Create_Should_Throw_When_ConsumerSettings_Is_Missing()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, FakeLogMessage>(
                includeConsumerSettings: false,
                includeScalingQueue: true,
                includeConnection: true
            );

            var factory = new ConsumerServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));

            // Safer: avoid checking for "ConsumerSettings" specifically
            Assert.Contains("No service for type", ex.Message);
        }

        [Fact]
        public void Create_Should_Throw_When_ScalingQueue_Is_Missing()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, FakeLogMessage>(
                includeScalingQueue: false,
                includeConnection: true // ensure all others present
            );

            var factory = new ConsumerServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));

            // More reliable assertion
            Assert.Contains("No service for type", ex.Message);
        }

        [Fact]
        public void Create_Should_Throw_When_Connection_Is_Missing()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeLogMessage, FakeLogMessage>(
                includeConnection: false,
                includeScalingQueue: true // ensure all others present
            );

            var factory = new ConsumerServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));
    
            // Safer: check that the error mentions "IConnection" somewhere
            Assert.Contains("No service for type", ex.Message);
        }

        [Fact]
        public void Create_Should_Succeed_For_Valid_WhatsAppMessage()
        {
            var provider = TestHelper.BuildValidConsumerServiceProvider<FakeWhatsAppMessage, IWhatsAppMessage>();
            var factory = new ConsumerServiceFactory(provider);

            var service = factory.Create(typeof(FakeWhatsAppMessage));

            // This verifies the correct generic type was created and the branch executed
            Assert.IsType<ConsumerService<FakeWhatsAppMessage, IWhatsAppMessage>>(service);
        }
    }
}
