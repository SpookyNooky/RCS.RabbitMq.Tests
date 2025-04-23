using RCS.RabbitMq.Consumer.Interfaces;
using RCS.RabbitMq.Logging.Processor.Interfaces;
using RCS.RabbitMq.Tests.Consumer.Models;

namespace RCS.RabbitMq.Tests.Consumer.Utilities
{
    public class StubConsumerServiceWithFixedTagReturningNonNull : IConsumerService<FakeLogMessage, ILogMessage>
    {
        public string StartConsuming(CancellationToken cancellationToken) => "valid-consumer-tag";
        public void StopConsuming(CancellationToken cancellationToken) { }
    }
}
