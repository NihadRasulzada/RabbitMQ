using RabbitMQ.Client;
using System.Text;

public class Program
{
    public static async Task Main(string[] args)
    {
    }

    public enum LogNames
    {
        Critical = 1,
        Error,
        Warning,
        Information,
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
    public async Task DirectExchangePublish()
    {
        // RabbitMQ bağlantısı için ConnectionFactory nesnesi oluşturuluyor
        ConnectionFactory factory = new ConnectionFactory();
        // AMQP URI üzerinden bağlantı parametreleri ayarlanıyor (Bağlantı dizesi ile şifre, kullanıcı adı, vb. bilgileri içeriyor)
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");

        // Bağlantıyı oluşturuyoruz
        using var connection = await factory.CreateConnectionAsync();
        // Kanal oluşturuluyor (RabbitMQ üzerinden mesajları gönderebilmek için kanal gerekli)
        var channel = await connection.CreateChannelAsync();

        // "logs-direct" adında bir exchange oluşturuluyor (Direct type, yani mesajlar belirli routing key'lere yönlendirilir)
        await channel.ExchangeDeclareAsync(exchange: "logs-direct",
                                           durable: true,   // Exchange'in kalıcı olmasını sağlıyoruz (RabbitMQ yeniden başlatıldığında bile mevcut kalır)
                                           type: ExchangeType.Direct);  // Direct Exchange türü, belirli routing key'lere yönlendirilir

        // LogNames enum'undaki her bir log tipi için bir kuyruk oluşturuluyor ve binding işlemi yapılıyor
        Enum.GetNames(typeof(LogNames)).ToList().ForEach(async x =>
        {
            var routeKey = $"route-{x}";  // Routing key, her log türü için benzersiz olacak
            var queueName = $"direct-queue-{x}";  // Kuyruk adı, log türüne bağlı olarak dinamik olarak isimlendiriliyor
            await channel.QueueDeclareAsync(queue: queueName, // Kuyruk oluşturuluyor
                                            durable: true,   // Kuyruk kalıcı olmalı
                                            exclusive: false,  // Kuyruk yalnızca bu bağlantıya özel olmasın
                                            autoDelete: false);  // Kuyruk otomatik silinmesin
            await channel.QueueBindAsync(queue: queueName,  // Kuyruk, exchange ile bağlanıyor
                                         exchange: "logs-direct",  // Mesajlar "logs-direct" exchange'ine yönlendirilir
                                         routingKey: routeKey);    // Routing key, ilgili kuyruğa yönlendirilecek mesajları belirler
        });

        // RabbitMQ üzerinden gönderilecek mesajlar için temel özellikler tanımlanıyor
        var basicProperties = new BasicProperties();

        // 50 adet log mesajı oluşturulup yayınlanıyor
        Enumerable.Range(1, 50).ToList().ForEach(async x =>
        {
            // Log türü rastgele seçiliyor (LogNames enum'undan bir değer)
            LogNames log = (LogNames)new Random().Next(1, 5);
            string message = $"log-type: {log}";  // Mesaj içeriği, log türünü içeriyor
            var messageBody = Encoding.UTF8.GetBytes(message);  // Mesajın byte array formatına dönüştürülmesi
            var routeKey = $"route-{log}";  // Routing key, mesajın hangi kuyruğa gideceğini belirliyor
            await channel.BasicPublishAsync(exchange: "logs-direct",  // Mesaj, "logs-direct" exchange'ine gönderiliyor
                                            routingKey: routeKey,   // Routing key ile yönlendirme yapılır
                                            mandatory: false,  // Mesajın başarıyla teslim edilmesi zorunlu değil
                                            basicProperties: basicProperties,  // Mesaj özellikleri
                                            body: messageBody);  // Mesaj içeriği
            Console.WriteLine($"Logs Published : {message}");  // Log yayınlandığına dair bilgi yazdırılıyor
        });

        // Programın sonlanmasını beklemek için bir ReadLine bekleniyor (terminalde sonucu görmek için)
        Console.ReadLine();

    }

    public async Task PublishTopicLogsAsync()
    {
        // RabbitMQ serverinə qoşulmaq üçün ConnectionFactory obyektini yaradırıq
        ConnectionFactory factory = new ConnectionFactory();
        // RabbitMQ serverinin URI-sini təyin edirik (bu, serverə necə qoşulacağımızı göstərir)
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");

        using var connection = await factory.CreateConnectionAsync();  // RabbitMQ serverinə qoşuluruq
        var channel = await connection.CreateChannelAsync();  // Kanal yaradılır
        await channel.ExchangeDeclareAsync("logs-topic", durable: true, type: ExchangeType.Topic);  // "logs-topic" adı ilə topic tipi üzrə exchange yaradılır

        var basicProperties = new BasicProperties();  // Mesaj üçün sadə əsas xüsusiyyətlər təyin edilir
        Random rnd = new Random();  // Random nömrələr yaratmaq üçün obyekt
        Enumerable.Range(1, 50).ToList().ForEach(async x =>  // 50 log mesajı göndəririk
        {
            // Random olaraq 3 fərqli log tipi seçilir
            LogNames log1 = (LogNames)rnd.Next(1, 5);
            LogNames log2 = (LogNames)rnd.Next(1, 5);
            LogNames log3 = (LogNames)rnd.Next(1, 5);
            var routeKey = $"{log1}.{log2}.{log3}";  // Routing key (log növlərini birləşdiririk)
            string message = $"log-type: {log1}-{log2}-{log3}";  // Mesajın mətni
            var messageBody = Encoding.UTF8.GetBytes(message);  // Mesajı byte formatına çeviririk
            await channel.BasicPublishAsync("logs-topic", routeKey, false, basicProperties, messageBody);  // Mesajı topic exchange'ə göndəririk
            Console.WriteLine($"Logs Sending : {message}");  // Hər bir logun göndərildiyi ekrana yazılır
        });

        // İstifadəçi proqramın bitməsini gözləyir
        Console.ReadLine();

    }

}
