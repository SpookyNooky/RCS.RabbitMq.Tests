using Microsoft.Extensions.Configuration;
using Moq;
using RCS.RabbitMq.Consumer.Factories;
using RCS.RabbitMq.Shared.Interfaces;

namespace RCS.RabbitMq.Tests.Consumer.Settings;

public class RabbitMqSettingsLoaderTests
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
            ["RCS.RabbitMQ:Host"] = "localhost",
            ["RCS.RabbitMQ:Username"] = "guest",
            ["RCS.RabbitMQ:Password"] = "guest",
            ["RCS.RabbitMQ:ExchangeName"] = "exchange",
            ["RCS.RabbitMQ:ExchangeType"] = "direct",
            ["RCS.RabbitMQ:QueueName"] = "queue",
            ["RCS.RabbitMQ:RoutingKey"] = "key",
            ["RCS.RabbitMQ:MaxRetry"] = "5",
            ["RCS.RabbitMQ:RetryQueueTimeToLive"] = "60000",
            ["RCS.RabbitMQ:MessageTimeToLive"] = "300000"
        };

        var config = BuildConfiguration(configValues);
        var loader = new RabbitMqSettingsLoader(_mockValidator.Object);

        // Act
        var result = loader.LoadSettings(config);

        // Assert
        Assert.Equal("localhost", result.Host);
        Assert.Equal("guest", result.Username);
        Assert.Equal("exchange", result.ExchangeName);
        Assert.Equal("queue", result.QueueName);
        Assert.Equal(5, result.MaxRetry);
        Assert.Equal(60000, result.RetryQueueTimeToLive);
        Assert.Equal(300000, result.MessageTimeToLive);
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_Missing_Required_Fields()
    {
        // Arrange
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMQ:Host"] = "localhost",
            ["RCS.RabbitMQ:ExchangeName"] = "exchange",
            ["RCS.RabbitMQ:ExchangeType"] = "direct",
            ["RCS.RabbitMQ:QueueName"] = "queue"
        };

        var config = BuildConfiguration(configValues);
        var loader = new RabbitMqSettingsLoader(_mockValidator.Object);

        // Act + Assert
        Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_Numeric_Values_Invalid()
    {
        // Arrange
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMQ:Host"] = "localhost",
            ["RCS.RabbitMQ:Username"] = "guest",
            ["RCS.RabbitMQ:Password"] = "guest",
            ["RCS.RabbitMQ:ExchangeName"] = "exchange",
            ["RCS.RabbitMQ:ExchangeType"] = "direct",
            ["RCS.RabbitMQ:QueueName"] = "queue",
            ["RCS.RabbitMQ:RoutingKey"] = "key",
            ["RCS.RabbitMQ:MaxRetry"] = "abc", // invalid
            ["RCS.RabbitMQ:RetryQueueTimeToLive"] = "abc", // invalid
            ["RCS.RabbitMQ:MessageTimeToLive"] = "300000"
        };

        var config = BuildConfiguration(configValues);
        var loader = new RabbitMqSettingsLoader(_mockValidator.Object);

        // Act + Assert
        var ex = Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
        Assert.Contains("RetryQueueTimeToLive", ex.Message);
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_MaxRetry_Not_Valid_Int()
    {
        // Arrange
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMQ:Host"] = "localhost",
            ["RCS.RabbitMQ:Username"] = "guest",
            ["RCS.RabbitMQ:Password"] = "guest",
            ["RCS.RabbitMQ:ExchangeName"] = "exchange",
            ["RCS.RabbitMQ:ExchangeType"] = "direct",
            ["RCS.RabbitMQ:QueueName"] = "queue",
            ["RCS.RabbitMQ:RoutingKey"] = "key",
            ["RCS.RabbitMQ:MaxRetry"] = "abc", // invalid
            ["RCS.RabbitMQ:RetryQueueTimeToLive"] = "60000",
            ["RCS.RabbitMQ:MessageTimeToLive"] = "300000"
        };

        var config = BuildConfiguration(configValues);
        var loader = new RabbitMqSettingsLoader(_mockValidator.Object);

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
        Assert.Contains("MaxRetry", ex.Message);
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_RetryQueueTimeToLive_Not_Valid_Int()
    {
        // Arrange
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMQ:Host"] = "localhost",
            ["RCS.RabbitMQ:Username"] = "guest",
            ["RCS.RabbitMQ:Password"] = "guest",
            ["RCS.RabbitMQ:ExchangeName"] = "exchange",
            ["RCS.RabbitMQ:ExchangeType"] = "direct",
            ["RCS.RabbitMQ:QueueName"] = "queue",
            ["RCS.RabbitMQ:RoutingKey"] = "key",
            ["RCS.RabbitMQ:MaxRetry"] = "5",
            ["RCS.RabbitMQ:RetryQueueTimeToLive"] = "xyz", // invalid
            ["RCS.RabbitMQ:MessageTimeToLive"] = "300000"
        };

        var config = BuildConfiguration(configValues);
        var loader = new RabbitMqSettingsLoader(_mockValidator.Object);

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
        Assert.Contains("RetryQueueTimeToLive", ex.Message);
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_MessageTimeToLive_Not_Valid_Int()
    {
        // Arrange
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMQ:Host"] = "localhost",
            ["RCS.RabbitMQ:Username"] = "guest",
            ["RCS.RabbitMQ:Password"] = "guest",
            ["RCS.RabbitMQ:ExchangeName"] = "exchange",
            ["RCS.RabbitMQ:ExchangeType"] = "direct",
            ["RCS.RabbitMQ:QueueName"] = "queue",
            ["RCS.RabbitMQ:RoutingKey"] = "key",
            ["RCS.RabbitMQ:MaxRetry"] = "5",
            ["RCS.RabbitMQ:RetryQueueTimeToLive"] = "60000",
            ["RCS.RabbitMQ:MessageTimeToLive"] = "notanumber" // invalid
        };

        var config = BuildConfiguration(configValues);
        var loader = new RabbitMqSettingsLoader(_mockValidator.Object);

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
        Assert.Contains("MessageTimeToLive", ex.Message);
    }

}
