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

public class TestConsumerService : ConsumerService<FakeLogMessage, ILogMessage>, IConsumerService<FakeLogMessage, ILogMessage>
{
    public TestConsumerService(IServiceProvider sp)
        : base(
            sp.GetRequiredService<IDataProcessingManager<FakeLogMessage>>(),
            sp.GetRequiredService<RabbitMqSettings>(),
            sp.GetRequiredService<ConsumerSettings>(),
            sp.GetRequiredService<ConcurrentQueue<Action>>(),
            sp.GetRequiredService<ILogger<ConsumerService<FakeLogMessage, ILogMessage>>>(),
            sp.GetRequiredService<IConnection>(),
            sp.GetRequiredService<ConsumerManager<FakeLogMessage, ILogMessage>>()
        )
    { }

    public override string? StartConsuming(CancellationToken cancellationToken) => null; // Simulate null tag
}