using Microsoft.Extensions.Configuration;
using Moq;
using RCS.RabbitMq.Consumer.Factories;
using RCS.RabbitMq.Shared.Interfaces;

namespace RCS.RabbitMq.Tests.Consumer.Settings;

public class ConsumerSettingsLoaderTests
{
    private readonly Mock<IConfigurationIntegrityValidator> _mockValidator = new();

    private static IConfiguration BuildConfiguration(Dictionary<string, string?> values)
    {
        return new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();
    }

    [Fact]
    public void LoadSettings_Should_Return_Populated_Settings()
    {
        // Arrange
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMq.Consumer:ServiceMappings:Log:MessageInterface"] = "ILogMessage",
            ["RCS.RabbitMq.Consumer:ServiceMappings:Log:MessageClass"] = "FakeLogMessage",
            ["RCS.RabbitMq.Consumer:ServiceMappings:Log:MessageMapper"] = "FakeMapper",
            ["RCS.RabbitMq.Consumer:MinConsumerCount"] = "1",
            ["RCS.RabbitMq.Consumer:MaxConsumerCount"] = "5",
            ["RCS.RabbitMq.Consumer:BatchSize"] = "10",
            ["RCS.RabbitMq.Consumer:InactivityTrigger"] = "1000",
            ["RCS.RabbitMq.Consumer:NonRetryableExceptions:0"] = "System.ArgumentException",
            ["RCS.RabbitMq.Consumer:NonRetryableExceptions:1"] = "System.InvalidOperationException"
        };

        var config = BuildConfiguration(configValues);
        var loader = new ConsumerSettingsLoader(_mockValidator.Object);

        // Act
        var result = loader.LoadSettings(config);

        // Assert
        Assert.Equal("Log", result.MessageType);
        Assert.Equal("ILogMessage", result.MessageInterface);
        Assert.Equal("FakeLogMessage", result.MessageClass);
        Assert.Equal("FakeMapper", result.MessageMapper);
        Assert.Equal(1, result.MinConsumerCount);
        Assert.Equal(5, result.MaxConsumerCount);
        Assert.Equal(10, result.BatchSize);
        Assert.Equal(1000, result.InactivityTrigger);
        Assert.Contains("System.ArgumentException", result.NonRetryableExceptions);
        Assert.Contains("System.InvalidOperationException", result.NonRetryableExceptions);
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_Required_Field_Missing()
    {
        // Arrange — Missing MessageClass
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMq.Consumer:ServiceMappings:Log:MessageInterface"] = "ILogMessage",
            // Missing MessageClass
            ["RCS.RabbitMq.Consumer:ServiceMappings:Log:MessageMapper"] = "FakeMapper",
            ["RCS.RabbitMq.Consumer:MinConsumerCount"] = "1",
            ["RCS.RabbitMq.Consumer:MaxConsumerCount"] = "5",
            ["RCS.RabbitMq.Consumer:BatchSize"] = "10",
            ["RCS.RabbitMq.Consumer:InactivityTrigger"] = "1000"
        };

        var config = BuildConfiguration(configValues);
        var loader = new ConsumerSettingsLoader(_mockValidator.Object);

        // Act + Assert
        var ex = Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
        Assert.Contains("MessageClass", ex.Message);
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_Numeric_Field_Is_Invalid()
    {
        // Arrange — MaxConsumerCount is invalid
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMq.Consumer:ServiceMappings:Log:MessageInterface"] = "ILogMessage",
            ["RCS.RabbitMq.Consumer:ServiceMappings:Log:MessageClass"] = "FakeLogMessage",
            ["RCS.RabbitMq.Consumer:ServiceMappings:Log:MessageMapper"] = "FakeMapper",
            ["RCS.RabbitMq.Consumer:MinConsumerCount"] = "1",
            ["RCS.RabbitMq.Consumer:MaxConsumerCount"] = "not-a-number", // invalid
            ["RCS.RabbitMq.Consumer:BatchSize"] = "10",
            ["RCS.RabbitMq.Consumer:InactivityTrigger"] = "1000"
        };

        var config = BuildConfiguration(configValues);
        var loader = new ConsumerSettingsLoader(_mockValidator.Object);

        // Act + Assert
        var ex = Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
        Assert.Contains("MaxConsumerCount", ex.Message);
    }
}
