using Microsoft.Extensions.Logging;
using Moq;
using RCS.RabbitMq.Consumer.Initialisation;
using RCS.RabbitMq.Consumer.Interfaces;

namespace RCS.RabbitMq.Tests.Consumer.Initialisation;

public class RabbitMqHostedInitialiserTests
{
    [Fact]
    public async Task StartAsync_Should_Call_Initialiser_And_Log_Info()
    {
        // Arrange
        var mockLogger = new Mock<ILogger<RabbitMqHostedInitialiser>>();
        var mockInitialiser = new Mock<IRabbitMqInitialiser>();
        var hostedService = new RabbitMqHostedInitialiser(mockInitialiser.Object, mockLogger.Object);

        var token = CancellationToken.None;

        // Act
        await hostedService.StartAsync(token);

        // Assert
        mockInitialiser.Verify(i => i.InitialiseExchangeAndQueue(), Times.Once);

        mockLogger.Verify(l => l.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("Initialising RabbitMQ infrastructure")),
            null,
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Once);

        mockLogger.Verify(l => l.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, _) => v.ToString()!.Contains("RabbitMQ infrastructure initialised")),
            null,
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.Once);
    }

    [Fact]
    public async Task StopAsync_Should_Complete_Silently()
    {
        // Arrange
        var mockLogger = new Mock<ILogger<RabbitMqHostedInitialiser>>();
        var mockInitialiser = new Mock<IRabbitMqInitialiser>();
        var hostedService = new RabbitMqHostedInitialiser(mockInitialiser.Object, mockLogger.Object);

        // Act
        await hostedService.StopAsync(CancellationToken.None);

        // Assert
        Assert.True(true); // Optional: Satisfies xUnit's assertion requirement
    }
}