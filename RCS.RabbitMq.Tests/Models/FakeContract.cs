using RCS.RabbitMq.Shared.Interfaces;

namespace RCS.RabbitMq.Tests.Consumer.Models;

public class FakeContract : IBasicMessageContract
{
    public ulong DeliveryTag { get; set; }
    public IDictionary<string, object>? Headers { get; set; } = new Dictionary<string, object>();
}