using RCS.RabbitMq.Logging.Processor.Interfaces;
using RCS.RabbitMq.Logging.Processor.Models;

namespace RCS.RabbitMq.Tests.Consumer.Models;

public class FakeLogMessage : LogMessage, ILogMessage;