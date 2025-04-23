using Microsoft.Extensions.Logging;
using Moq;
using RCS.RabbitMq.Consumer.Managers;

namespace RCS.RabbitMq.Tests.Consumer.Managers;

public class ScalingCommandHandlerTests
{
    [Fact]
    public void AddCommand_Should_Store_And_Execute_Command()
    {
        // Arrange
        var logger = new Mock<ILogger<ScalingCommandHandler>>();
        var handler = new ScalingCommandHandler(logger.Object);

        var wasExecuted = false;

        // Act
        handler.AddCommand(() => wasExecuted = true);
        handler.ExecuteCommands();

        // Assert
        Assert.True(wasExecuted);
    }

    [Fact]
    public void ExecuteCommands_Should_Run_All_Actions()
    {
        // Arrange
        var logger = new Mock<ILogger<ScalingCommandHandler>>();
        var handler = new ScalingCommandHandler(logger.Object);

        var executed1 = false;
        var executed2 = false;

        handler.AddCommand(() => executed1 = true);
        handler.AddCommand(() => executed2 = true);

        // Act
        handler.ExecuteCommands();

        // Assert
        Assert.True(executed1);
        Assert.True(executed2);
    }

    [Fact]
    public void ExecuteCommands_Should_Continue_After_Exception()
    {
        // Arrange
        var logger = new Mock<ILogger<ScalingCommandHandler>>();
        var handler = new ScalingCommandHandler(logger.Object);

        var executed = false;

        handler.AddCommand(() => throw new Exception("bang"));
        handler.AddCommand(() => executed = true);

        // Act
        handler.ExecuteCommands();

        // Assert
        Assert.True(executed);
    }

    [Fact]
    public void ExecuteScalingCommands_Should_Run_Queued_Actions()
    {
        // Arrange
        var logger = new Mock<ILogger<ScalingCommandHandler>>();
        var handler = new ScalingCommandHandler(logger.Object);
        var executed = false;

        handler.AddCommand(() => executed = true);

        // Act
        handler.ExecuteCommands();

        // Assert
        Assert.True(executed, "Expected the queued action to be executed.");
    }

    [Fact]
    public void StripGeneratedPart_Should_Return_MethodName_When_No_Generics()
    {
        // Arrange
        var logger = new Mock<ILogger<ScalingCommandHandler>>();
        var handler = new ScalingCommandHandler(logger.Object);

        // Act
        handler.AddCommand(NamedPlainMethod);
        handler.ExecuteCommands();

        // Assert
        logger.Verify(l => l.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("NamedPlainMethod")),
            null,
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()), Times.AtLeastOnce);
    }

    private static void NamedPlainMethod()
    {
        // simple placeholder method for command
    }
}