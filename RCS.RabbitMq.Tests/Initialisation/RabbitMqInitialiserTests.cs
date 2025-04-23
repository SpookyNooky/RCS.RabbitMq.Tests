using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using RCS.RabbitMq.Consumer.Initialisation;
using RCS.RabbitMq.Consumer.Models;

namespace RCS.RabbitMq.Tests.Consumer.Initialisation
{
    public class RabbitMqInitialiserTests
    {
        [Fact]
        public void InitialiseExchangeAndQueue_Should_Declare_Exchange_And_Queue()
        {
            // Arrange
            var settings = new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                QueueName = "test.queue",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                RoutingKey = "test",
                RetryQueueTimeToLive = 60000
            };

            var mockLogger = new Mock<ILogger<RabbitMqInitialiser>>();
            var mockChannel = new Mock<IModel>();

            var initialiser = new RabbitMqInitialiser(mockChannel.Object, settings, mockLogger.Object);

            // Act
            initialiser.InitialiseExchangeAndQueue();

            // Assert (sample assertion)
            mockChannel.Verify(c => c.ExchangeDeclare(
                settings.ExchangeName.ToLower(),
                settings.ExchangeType.ToLower(),
                true,
                false,
                null), Times.Once);
        }

        [Fact]
        public void InitialiseExchangeAndQueue_Should_LogError_And_Rethrow_On_Exception()
        {
            // Arrange
            var settings = new RabbitMqSettings
            {
                Host = "localhost",
                Username = "guest",
                Password = "guest",
                QueueName = "test.queue",
                ExchangeName = "test.exchange",
                ExchangeType = "direct",
                RoutingKey = "test",
                RetryQueueTimeToLive = 60000
            };

            var mockLogger = new Mock<ILogger<RabbitMqInitialiser>>();
            var mockChannel = new Mock<IModel>();

            // Force failure on ExchangeDeclare
            mockChannel
                .Setup(c => c.ExchangeDeclare(It.IsAny<string>(), It.IsAny<string>(), true, false, null))
                .Throws(new InvalidOperationException("Simulated failure"));

            var initialiser = new RabbitMqInitialiser(mockChannel.Object, settings, mockLogger.Object);

            // Act + Assert
            var ex = Assert.Throws<InvalidOperationException>(() => initialiser.InitialiseExchangeAndQueue());
            Assert.Equal("Simulated failure", ex.Message);

            // Verify LogError was called
            mockLogger.Verify(l => l.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Error during RabbitMQ initialisation")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Once);
        }
    }
}