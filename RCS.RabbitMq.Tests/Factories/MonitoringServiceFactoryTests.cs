using RCS.RabbitMq.Consumer.Factories;
using RCS.RabbitMq.Logging.Processor.Interfaces;
using RCS.RabbitMq.Tests.Consumer.Models;
using RCS.RabbitMq.Tests.Consumer.Utilities;
using RCS.RabbitMQ.WhatsApp.ActionLog.Processor.Interfaces;

namespace RCS.RabbitMq.Tests.Consumer.Factories
{
    public class MonitoringServiceFactoryTests
    {
        [Fact]
        public void Create_Should_Throw_For_Unknown_MessageType()
        {
            var provider = TestHelper.BuildValidServiceProvider<FakeLogMessage, ILogMessage>();
            var factory = new MonitoringServiceFactory(provider);
            var unknownType = typeof(string);

            Assert.Throws<InvalidOperationException>(() => factory.Create(unknownType));
        }

        [Fact]
        public void Create_Should_Succeed_For_Valid_LogMessage()
        {
            var provider = TestHelper.BuildValidServiceProvider<FakeLogMessage, ILogMessage>();
            var factory = new MonitoringServiceFactory(provider);

            var ex = Record.Exception(() => factory.Create(typeof(FakeLogMessage)));
            Assert.Null(ex);
        }

        [Fact]
        public void Create_Should_Succeed_For_Valid_WhatsAppMessage()
        {
            var provider = TestHelper.BuildValidServiceProvider<FakeWhatsAppMessage, IWhatsAppMessage>();
            var factory = new MonitoringServiceFactory(provider);

            var ex = Record.Exception(() => factory.Create(typeof(FakeWhatsAppMessage)));
            Assert.Null(ex);
        }

        [Fact]
        public void Create_Should_Throw_When_Logger_Is_Missing()
        {
            var provider = TestHelper.BuildValidServiceProvider<FakeLogMessage, ILogMessage>(includeLogger: false);
            var factory = new MonitoringServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));
            Assert.Contains("ILogger", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Create_Should_Throw_When_ConsumerManager_Is_Missing()
        {
            var provider = TestHelper.BuildValidServiceProvider<FakeLogMessage, ILogMessage>(includeConsumerManager: false);
            var factory = new MonitoringServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));
            Assert.Contains("ConsumerManager", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Create_Should_Throw_When_RabbitMqSettings_Is_Missing()
        {
            var provider = TestHelper.BuildValidServiceProvider<FakeLogMessage, ILogMessage>(includeRabbitMqSettings: false);
            var factory = new MonitoringServiceFactory(provider);

            var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(typeof(FakeLogMessage)));
            Assert.Contains("RabbitMqSettings", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
