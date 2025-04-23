using RCS.RabbitMq.Shared.Interfaces;

namespace RCS.RabbitMq.Tests.Consumer.Models;

public class FakeMapper : IRabbitMessageMapper<FakeMessage, FakeContract>
{
    public FakeContract Map(FakeMessage _) => new();

    public static FakeContract ToContract(FakeMessage _) => new();
    public static FakeMessage ToMessage(FakeContract _) => new();
}