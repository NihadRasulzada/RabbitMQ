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
    // Bu metot mesajları RabbitMQ'ya gönderir.
    public async Task SendMessagesAsync()
    {
        // RabbitMQ bağlantısı için ConnectionFactory nesnesi oluşturuluyor
        ConnectionFactory factory = new ConnectionFactory();

        // AMQP URI üzerinden bağlantı parametreleri ayarlanıyor
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");

        // Bağlantı oluşturuluyor ve IConnection nesnesi üzerinden bağlantı sağlanıyor
        using (IConnection connection = await factory.CreateConnectionAsync())
        {
            // Kanal oluşturuluyor ve bağlantıya bağlanılıyor
            IChannel channel = await connection.CreateChannelAsync();

            // "logs-fanout" adında bir exchange oluşturuluyor, türü "fanout" ve dayanıklı (durable) olarak ayarlanıyor
            await channel.ExchangeDeclareAsync(exchange: "logs-fanout",
                                               type: ExchangeType.Fanout,
                                               durable: true);

            // Mesaj özellikleri oluşturuluyor
            var basicProperties = new BasicProperties();

            // 1 ile 50 arasındaki sayılar için bir mesaj yayınlanıyor
            Enumerable.Range(1, 50).ToList().ForEach(async x =>
            {
                // Gönderilecek mesaj formatı oluşturuluyor
                string message = $"Log {x}";

                // Mesaj string'ini byte dizisine dönüştürülüyor
                byte[] body = Encoding.UTF8.GetBytes(message);

                // Mesaj RabbitMQ kanalına gönderiliyor (publish ediliyor)
                await channel.BasicPublishAsync(
                    exchange: "logs-fanout", // Mesajın gönderileceği exchange adı
                    routingKey: string.Empty, // Fanout exchange olduğu için routingKey kullanılmıyor
                    mandatory: false,  // Mesajın ulaşması zorunlu değil
                    basicProperties: basicProperties, // Mesajın özellikleri
                    body: body); // Mesajın gövdesi (byte dizisi)

                // Konsola mesaj gönderildiği bilgisi yazdırılıyor
                Console.WriteLine($" [x] Sent {message}");
            });
        }
    }
}
