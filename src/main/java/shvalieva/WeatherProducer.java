package shvalieva;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class WeatherProducer {
    private static final String TOPIC = "weather";
    private static final String[] CITIES = {"Москва", "Казань", "Симферополь", "Киров", "Волгоград"};
    private static final Random RANDOM = new Random();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public void generateAndSend() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        LocalDate startDate = LocalDate.of(2020, 1, 1);

        while (true) {
            for (String city : CITIES) {
                WeatherData data = new WeatherData(
                        city,
                        -40 + RANDOM.nextDouble() * 70,
                        20 + RANDOM.nextInt(80),
                        RANDOM.nextBoolean(),
                        startDate.format(DateTimeFormatter.ISO_LOCAL_DATE)
                );
                String json = MAPPER.writeValueAsString(data);
                producer.send(new ProducerRecord<>(TOPIC, city, json));
                System.out.println("Сообщение отправлено в кафку: " + json);
            }
            Thread.sleep(2000);
            startDate = startDate.plusDays(1);
        }
    }
}

