using RCS.RabbitMQ.WhatsApp.ActionLog.Processor.Interfaces;
using RCS.RabbitMQ.WhatsApp.ActionLog.Processor.Models;

namespace RCS.RabbitMq.Tests.Consumer.Models
{
    public class FakeWhatsAppMessage : WhatsAppMessage, IWhatsAppMessage { }
}
