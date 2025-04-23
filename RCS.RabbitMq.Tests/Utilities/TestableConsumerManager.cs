using Microsoft.Extensions.Logging;
using RCS.RabbitMq.Consumer.Managers;
using RCS.RabbitMq.Consumer.Services;
using RCS.RabbitMq.Logging.Processor.Interfaces;
using RCS.RabbitMq.Shared.Interfaces;
using RCS.RabbitMq.Tests.Consumer.Models;

namespace RCS.RabbitMq.Tests.Consumer.Utilities;

public class TestableConsumerManager : ConsumerManager<FakeLogMessage, ILogMessage>
{
    private readonly ConsumerService<FakeLogMessage, ILogMessage> _stub;

    public TestableConsumerManager(
        ScalingCommandHandler handler,
        IServiceProvider provider,
        ILogger<ConsumerManager<FakeLogMessage, ILogMessage>> logger,
        IRabbitMessageMapper<FakeLogMessage, ILogMessage> mapper,
        ConsumerService<FakeLogMessage, ILogMessage> stub)
        : base(handler, provider, logger, mapper)
    {
        _stub = stub;
    }

    protected override ConsumerService<FakeLogMessage, ILogMessage> CreateConsumerService() => _stub;
}