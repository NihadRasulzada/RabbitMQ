using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace RabbitMQ.WatermarkApp.Services
{
    public class RabbitMQPublisher
    {
        private readonly RabbitMQClientService _rabbitMQClientService;

        public RabbitMQPublisher(RabbitMQClientService rabbitMQClientService)
        {
            _rabbitMQClientService = rabbitMQClientService;
        }

        public async Task PublishAsync(productImageCreatedEvent productImageCreatedEvent)
        {
            var channel = await _rabbitMQClientService.ConnectAsync();

            var bodyString = JsonSerializer.Serialize(productImageCreatedEvent);

            var bodyByte = Encoding.UTF8.GetBytes(bodyString);

            var properties = new BasicProperties();
            properties.Persistent = true;

            await channel.BasicPublishAsync(mandatory: false,
                                            exchange: RabbitMQClientService.ExchangeName,
                                            routingKey: RabbitMQClientService.RoutingWatermark,
                                            basicProperties: properties,
                                            body: bodyByte); 
        }
    }
}
