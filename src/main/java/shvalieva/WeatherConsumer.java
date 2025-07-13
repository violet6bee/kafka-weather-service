package shvalieva;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class WeatherConsumer {
    private static final String TOPIC = "weather";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public void readAndAnalyze(String cityFilter) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "weather-analytics-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        List<WeatherData> cityData = new ArrayList<>();

        System.out.println("Чтение сообщений из кафки: " + cityFilter);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            if (records.isEmpty()) continue;

            for (ConsumerRecord<String, String> record : records) {
                try {
                    WeatherData data = MAPPER.readValue(record.value(), WeatherData.class);
                    if (data.city.equalsIgnoreCase(cityFilter)) {
                        cityData.add(data);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

            LocalDate maxDate = cityData.stream()
                    .map(d -> LocalDate.parse(d.date, formatter))
                    .max(LocalDate::compareTo)
                    .orElse(null);

            if (maxDate == null) {
                System.out.println("Нет данных для анализа.");
                return;
            }

            LocalDate fromDate = maxDate.minusDays(6);
            List<WeatherData> recent = cityData.stream()
                    .filter(d -> {
                        LocalDate date = LocalDate.parse(d.date, formatter);
                        return !date.isBefore(fromDate);
                    })
                    .toList();

            if (recent.isEmpty()) {
                System.out.println("Нет данных за последние 7 дней.");
                return;
            }

            double avgTemp = recent.stream().mapToDouble(d -> d.temperature).average().orElse(0);
            long cloudyDays = recent.stream().filter(d -> d.cloudy).count();

            System.out.printf("Аналитика по городу %s за последние 7 дней:\n", cityFilter);
            System.out.printf("Средняя температура: %.2f°C\n", avgTemp);
            System.out.printf("Количество облачных дней: %d\n", cloudyDays);
        }


    }
}

