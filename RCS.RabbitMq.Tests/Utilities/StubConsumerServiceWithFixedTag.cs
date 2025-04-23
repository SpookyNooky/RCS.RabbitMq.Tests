using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RCS.RabbitMq.Consumer.Interfaces;
using RCS.RabbitMq.Consumer.Managers;
using RCS.RabbitMq.Consumer.Models;
using RCS.RabbitMq.Consumer.Services;
using RCS.RabbitMq.Logging.Processor.Interfaces;
using RCS.RabbitMq.Tests.Consumer.Models;

namespace RCS.RabbitMq.Tests.Consumer.Utilities;

public class StubConsumerServiceWithFixedTag(
    IServiceProvider provider,
    string tag) : ConsumerService<FakeLogMessage, ILogMessage>(
    provider.GetRequiredService<IDataProcessingManager<FakeLogMessage>>(),
    provider.GetRequiredService<RabbitMqSettings>(),
    provider.GetRequiredService<ConsumerSettings>(),
    provider.GetRequiredService<ConcurrentQueue<Action>>(),
    provider.GetRequiredService<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
    provider.GetRequiredService<IConnection>(),
    provider.GetRequiredService<ConsumerManager<FakeLogMessage, ILogMessage>>())
{
    public override string? StartConsuming(CancellationToken cancellationToken) => tag;
}