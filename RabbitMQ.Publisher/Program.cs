using RabbitMQ.Client;  
using System.Text;  

public class Program 
{
    public static async Task Main(string[] args)  
    {
        
    }
    public async Task PublishMessagesToQueueAsync()
    {
        ConnectionFactory factory = new ConnectionFactory();  // RabbitMQ serverinə qoşulma üçün fabrik sinifi
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");  // RabbitMQ serverinin URI-sini təyin edir

        using (IConnection connection = await factory.CreateConnectionAsync())  // RabbitMQ serverinə asinxron qoşulma
        {
            IChannel channel = await connection.CreateChannelAsync();  // Bağlantıdan bir kanal (channel) yaradır
            await channel.QueueDeclareAsync(queue: "hello-queue",  // "hello-queue" adlı bir sıra (queue) elan edir
                                            durable: true,  // Sıra davamlı olacaq, yəni server restart olarsa, məlumatlar itməyəcək
                                            exclusive: false,  // Bu sıra yalnız bu bağlantı üçün məhdud olmayacaq
                                            autoDelete: false);  // Sıra heç bir istehsaldan sonra avtomatik olaraq silinməyəcək

            var basicProperties = new BasicProperties();  // Mesajın əsas xüsusiyyətlərini saxlamaq üçün obyekt

            // Aşağıda şərh edilmiş hissə, mesajın davamlı olmasını təmin etmək üçün istifadə edilə bilərdi
            //basicProperties.Persistent = true;  // Mesajın davamlı olması üçün (server restart olarsa da məlumat itməsin)


            Enumerable.Range(1, 50).ToList().ForEach(async x =>
            {
                string message = $"Message {x}";  // Hər bir mesajın məzmununu təyin edir
                byte[] body = Encoding.UTF8.GetBytes(message);  // Mesajı UTF-8 ilə byte dizisinə çevirir

                await channel.BasicPublishAsync(  // Mesajı sıraya göndərir
                    exchange: string.Empty,  // Məlumatı birbaşa qeydiyyatdan keçirir (default exchange istifadə edilir)
                    routingKey: "hello-queue",  // Mesajın gedəcəyi sıra
                    mandatory: false,  // Məcburi göndərmə deyil, əgər hədəf mövcud deyilsə, mesaj itə bilər
                    basicProperties: basicProperties,  // Mesajın xüsusiyyətləri (məsələn, davamlılıq)
                    body: body);  // Mesajın məzmunu

                Console.WriteLine($" [x] Sent {message}");
            });

            Console.ReadLine();
        }
    }
}
