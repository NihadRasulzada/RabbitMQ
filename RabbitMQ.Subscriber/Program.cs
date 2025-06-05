using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

internal class Program
{
    private static async Task Main(string[] args)
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
}