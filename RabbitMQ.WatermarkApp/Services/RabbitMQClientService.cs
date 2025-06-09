using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Threading.Tasks;

namespace RabbitMQ.WatermarkApp.Services
{
    public class RabbitMQClientService : IAsyncDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private IConnection _connection;
        private IChannel _channel;
        public static string ExchangeName = "ImageDirectExchange";
        public static string RoutingWatermark = "watermark-route-image";
        public static string QueueName = "queue-watermark-image";

        private readonly ILogger<RabbitMQClientService> _logger;

        public RabbitMQClientService(ConnectionFactory connectionFactory, ILogger<RabbitMQClientService> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
        }

        public async Task<IChannel> ConnectAsync()
        {
            try
            {
                // Ensure the connection is not null
                _connection = await _connectionFactory.CreateConnectionAsync();

                if (_channel is null || !_channel.IsOpen)
                {
                    _channel = await _connection.CreateChannelAsync();
                    await _channel.ExchangeDeclareAsync(ExchangeName, "direct", durable: true, autoDelete: false);
                    await _channel.QueueDeclareAsync(QueueName, durable: true, exclusive: false, autoDelete: false);
                    await _channel.QueueBindAsync(QueueName, ExchangeName, RoutingWatermark);

                    _logger.LogInformation("Connection established with RabbitMQ...");
                }

                return _channel;
            }
            catch (Exception ex)
            {
                _logger.LogError($"RabbitMQ connection error: {ex.Message}");
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_channel != null && _channel.IsOpen)
                {
                    await _channel.CloseAsync();
                    _channel.Dispose();
                    _logger.LogInformation("Connection with RabbitMQ closed...");
                }

                if (_connection != null && _connection.IsOpen)
                {
                    await _connection.CloseAsync();
                    _connection.Dispose();
                    _logger.LogInformation("RabbitMQ connection closed...");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error closing RabbitMQ connection: {ex.Message}");
            }
        }
    }
}
