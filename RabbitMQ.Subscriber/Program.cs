using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

public class Program
{
    public static async Task Main(string[] args)
    {
    }

    public async Task StartConsumingMessagesAsync()
    {
        ConnectionFactory factory = new ConnectionFactory();  // RabbitMQ serverinə qoşulma üçün fabrik sinifi
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");  // RabbitMQ serverinin URI-sini təyin edir

        using (IConnection connection = await factory.CreateConnectionAsync())  // RabbitMQ serverinə asinxron qoşulma
        {
            IChannel channel = await connection.CreateChannelAsync();  // Bağlantıdan bir kanal (channel) yaradır

            // Prefetch parametrlərini təyin edir ki, hər bir istehsalçıya ən çox 5 mesaj göndərilsin
            await channel.BasicQosAsync(prefetchSize: 0,  // Mesajların ölçüsünü təyin edir, burada 0 - ölçü məhdudiyyəti yoxdur
                                        prefetchCount: 5,  // Hər bir istehsalçıya göndəriləcək maksimal mesaj sayı
                                        global: false);  // Global QoS tətbiq etməmək, yalnız bu kanalda tətbiq edilir

            var consumer = new AsyncEventingBasicConsumer(channel);  // Asinxron bir istehlakçı (consumer) obyekti yaradılır

            // "hello-queue" adlandırılan sıradan mesajları istehlak etmək üçün bu metod istifadə edilir
            await channel.BasicConsumeAsync(queue: "hello-queue",  // Hedef sıra adı
                                            autoAck: false,  // Mesajın avtomatik olaraq təsdiq edilməməsi
                                            consumer: consumer);  // İstehlakçı obyektini təyin edir

            // Mesaj alındıqda bu hadisə işləyir
            consumer.ReceivedAsync += async (object sender, BasicDeliverEventArgs e) =>
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());  // Mesajın bədənini UTF-8 ilə oxuyur
                Console.WriteLine($" [x] Received {message}");

                // Mesaj alındıqdan sonra təsdiq edilir (acknowledgement)
                await channel.BasicAckAsync(e.DeliveryTag, true);  // Mesajın alınması təsdiq edilir
            };

            Console.ReadLine();
        }
    }
    // Bu metot mesajları qəbul etmək üçün RabbitMQ kanalını konfiqurasiya edir
    public async Task FanoutExchangeConsume()
    {
        // RabbitMQ serverinə qoşulmaq üçün ConnectionFactory obyektini yaradırıq
        ConnectionFactory factory = new ConnectionFactory();

        // RabbitMQ serverinin URI-sini təyin edirik (bu, serverə necə qoşulacağımızı göstərir)
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");

        // RabbitMQ serverinə asinxron qoşuluruq
        using (IConnection connection = await factory.CreateConnectionAsync())
        {
            // Bağlantıdan bir kanal yaradırıq (kanal mesajların göndərilməsi və alınması üçün istifadə olunur)
            IChannel channel = await connection.CreateChannelAsync();

            // Kanal üzərindən yeni bir növbə (queue) yaradırıq və onun adını random olaraq təyin edirik
            string randomQueueName = (await channel.QueueDeclareAsync()).QueueName;

            // Növbəni "logs-fanout" adlı exchange ilə bağlayırıq (fanout exchange, bütün mesajları bağlanan bütün növbələrə göndərir)
            await channel.QueueBindAsync(queue: randomQueueName,
                                          exchange: "logs-fanout",
                                          routingKey: "",   // RoutingKey boş olduğu üçün, fanout exchange-də bütün növbələrə göndərilir
                                          arguments: null); // Əlavə arqumentlər yoxdur

            // Basic QoS (Quality of Service) ayarlarını təyin edirik:
            // Burada, yalnız bir mesajın işlənməsini istəyirik (prefetchCount: 1) və bu, mesajın əl ilə təsdiqlənməsini təmin edir
            await channel.BasicQosAsync(prefetchSize: 0,    // Mesajın ölçüsünü təyin edirik (0 - limitsiz)
                                        prefetchCount: 1,   // Hər seferində yalnız bir mesaj alınacaq
                                        global: false);     // Bu ayar yalnız bu kanal üçün keçərlidir

            // AsyncEventingBasicConsumer obyektini yaradırıq (bu, mesajları asinxron qəbul edəcək)
            var consumer = new AsyncEventingBasicConsumer(channel);

            // Növbəni dinləməyə başlayırıq və qəbul edilən mesajları async metod ilə işləyirik
            await channel.BasicConsumeAsync(queue: randomQueueName,
                                             autoAck: false,  // Mesaj avtomatik təsdiqlənmir, əl ilə təsdiq edilir
                                             consumer: consumer);  // Consumer obyektini təyin edirik

            // Konsol yazısı ilə mesajların qəbul edilməsini bildiririk
            Console.WriteLine("Logs Listening");

            // Mesaj alındıqda bu hadisə tetiklenir
            consumer.ReceivedAsync += async (object sender, BasicDeliverEventArgs e) =>
            {
                // Mesajın bədənini UTF-8 formatında oxuyuruq
                var message = Encoding.UTF8.GetString(e.Body.ToArray());

                // Mesajın işlənməsi üçün 1 saniyəlik gözləmə (simulyasiya məqsədilə)
                Thread.Sleep(1000);

                // Konsola mesajı yazdırırıq
                Console.WriteLine($" [x] Received {message}");

                // Mesajın işlənməsinin tamamlandığını RabbitMQ serverinə bildiririk (acknowledgement)
                await channel.BasicAckAsync(e.DeliveryTag, false);  // Mesajın uğurla qəbul edildiyini təsdiqləyirik
            };

            // Konsoldan daxil edilən hər hansı bir girişi gözləyirik (bu, proqramın dayanmasını təmin edir)
            Console.ReadLine();
        }
    }
    public async Task DirectExchangeConsume()
    {
        // RabbitMQ serverinə qoşulmaq üçün ConnectionFactory obyektini yaradırıq
        ConnectionFactory factory = new ConnectionFactory();
        // RabbitMQ serverinin URI-sini təyin edirik (bu, serverə necə qoşulacağımızı göstərir)
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");
        using var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.BasicQosAsync(prefetchSize: 0,    // Mesajın ölçüsünü təyin edirik (0 - limitsiz)
                                    prefetchCount: 1,   // Hər seferində yalnız bir mesaj alınacaq
                                    global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        var queueName = "direct-queue-Critical";

        // Düzgün şəkildə consume edilməsi üçün "await" əlavə edilir
        await channel.BasicConsumeAsync(queue: queueName,
                                        autoAck: false,
                                        consumer: consumer);

        // `ReceivedAsync` hadisəsi düzgün şəkildə bağlanmalı
        consumer.ReceivedAsync += async (object sender, BasicDeliverEventArgs e) =>
        {
            var message = Encoding.UTF8.GetString(e.Body.ToArray());
            Console.WriteLine("Gelen Mesaj: " + message);
            // Task.Delay ilə simulasiya edilən gecikmə
            await Task.Delay(1000);  // Mesajın işlənməsi üçün 1 saniyəlik gözləmə (simulyasiya məqsədilə)

            // Mesaj işləndikdən sonra ack göndərilir
            await channel.BasicAckAsync(e.DeliveryTag, false);
        };

        // Konsolda məlumatı gözləməyə davam edirik
        Console.WriteLine("Logs Listening...");
        Console.ReadLine();
    }
    public async Task TopicExchangeConsume()
    {
        // RabbitMQ serverinə qoşulmaq üçün ConnectionFactory obyektini yaradırıq
        ConnectionFactory factory = new ConnectionFactory();
        // RabbitMQ serverinin URI-sini təyin edirik (bu, serverə necə qoşulacağımızı göstərir)
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");

        // RabbitMQ serverinə qoşuluruq
        using var connection = await factory.CreateConnectionAsync();
        // Kanal yaradılır
        var channel = await connection.CreateChannelAsync();

        // QoS (Quality of Service) parametrləri təyin edirik: hər seferində yalnız 1 mesaj qəbul edilsin
        await channel.BasicQosAsync(0, 1, false);

        // Mesajları dinləmək üçün consumer obyektini yaradırıq
        var consumer = new AsyncEventingBasicConsumer(channel);

        // Yeni bir növbə yaradılır və adı əldə edilir
        var queueName = (await channel.QueueDeclareAsync()).QueueName;

        // Bu routing key ilə mənbə növbəsi ilə bağlanırıq
        var routekey = "Info.#";  // Bu, "Info." ilə başlayan bütün routing key-ləri qəbul edəcək
        await channel.QueueBindAsync(queueName, "logs-topic", routekey);

        // Mesajları qəbul etməyə başlayırıq
        await channel.BasicConsumeAsync(queueName, false, consumer);

        Console.WriteLine("Logs Listening..."); // Dinləyici işə salındı, mesajlar gözlənir

        // Consumer obyektinin ReceivedAsync hadisəsinə mesaj alındıqda işləyən kodu əlavə edirik
        consumer.ReceivedAsync += async (object sender, BasicDeliverEventArgs e) =>
        {
            // Göndərilən mesajı byte array-dən string-ə çeviririk
            var message = Encoding.UTF8.GetString(e.Body.ToArray());

            // Bir neçə saniyəlik gecikmə əlavə edirik (şəxsi məqsədlər üçün istifadə edilə bilər)
            Thread.Sleep(1500);

            // Mesajı ekranda çap edirik
            Console.WriteLine("Messages:" + message);

            // Mesajın düzgün alındığını RabbitMQ-ya bildirmək üçün ACK (acknowledgment) göndəririk
            await channel.BasicAckAsync(e.DeliveryTag, false);
        };

        // Programın bitməsini gözləyirik
        Console.ReadLine();

    }
    public async Task HeaderExchangeConsume()
    {
        // RabbitMQ serverinə qoşulmaq üçün ConnectionFactory obyektini yaradırıq
        ConnectionFactory factory = new ConnectionFactory();
        // RabbitMQ serverinin URI-sini təyin edirik (bu, serverə necə qoşulacağımızı göstərir)
        factory.Uri = new Uri("amqps://jqtpztoz:EGeJb2LSQrMdWnmPeSJveypSJNNqkl3j@duck.lmq.cloudamqp.com/jqtpztoz");

        using var connection = await factory.CreateConnectionAsync();

        var channel = await connection.CreateChannelAsync();
        await channel.ExchangeDeclareAsync("header-exchange", durable: true, type: ExchangeType.Headers);

        await channel.BasicQosAsync(0, 1, false);
        var consumer = new AsyncEventingBasicConsumer(channel);

        var queueName = (await channel.QueueDeclareAsync()).QueueName;

        Dictionary<string, object> headers = new Dictionary<string, object>();

        headers.Add("format", "pdf");
        headers.Add("shape", "a4");
        headers.Add("x-match", "any");



        await channel.QueueBindAsync(queueName, "header-exchange", String.Empty, headers);

        await channel.BasicConsumeAsync(queueName, false, consumer);


        Console.WriteLine("Logları dinleniyor...");

        consumer.ReceivedAsync += async (object sender, BasicDeliverEventArgs e) =>
        {
            var message = Encoding.UTF8.GetString(e.Body.ToArray());

            Thread.Sleep(1500);
            Console.WriteLine("Gelen Mesaj:" + message);



            await channel.BasicAckAsync(e.DeliveryTag, false);
        };





        Console.ReadLine();

    }
}