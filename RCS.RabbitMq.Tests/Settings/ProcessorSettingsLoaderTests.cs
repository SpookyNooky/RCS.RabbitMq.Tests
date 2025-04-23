using Microsoft.Extensions.Configuration;
using RCS.RabbitMq.Consumer.Factories;

namespace RCS.RabbitMq.Tests.Consumer.Settings;

public class ProcessorSettingsLoaderTests
{
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
            ["RCS.RabbitMq.Consumer.Processor:ProcessorAssembly"] = "MyAssembly.dll",
            ["RCS.RabbitMq.Consumer.Processor:ProcessorClass"] = "MyNamespace.MyClass"
        };

        var config = BuildConfiguration(configValues);
        var loader = new ProcessorSettingsLoader();

        // Act
        var result = loader.LoadSettings(config);

        // Assert
        Assert.Equal("MyAssembly.dll", result.ProcessorAssembly);
        Assert.Equal("MyNamespace.MyClass", result.ProcessorClass); // FIXED
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_Assembly_Missing()
    {
        // Arrange — missing ProcessorAssembly
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMq.Consumer.Processor:Class"] = "MyNamespace.MyClass"
        };

        var config = BuildConfiguration(configValues);
        var loader = new ProcessorSettingsLoader();

        // Act + Assert
        var ex = Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
        Assert.Contains("ProcessorAssembly", ex.Message);
    }

    [Fact]
    public void LoadSettings_Should_Throw_If_Class_Missing()
    {
        // Arrange — missing Class
        var configValues = new Dictionary<string, string?>
        {
            ["RCS.RabbitMq.Consumer.Processor:ProcessorAssembly"] = "MyAssembly.dll"
        };

        var config = BuildConfiguration(configValues);
        var loader = new ProcessorSettingsLoader();

        // Act + Assert
        var ex = Assert.Throws<ArgumentException>(() => loader.LoadSettings(config));
        Assert.Contains("Class", ex.Message);
    }
}
