package shvalieva;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDate;

public class WeatherData {
    public String city;
    public double temperature;
    public int humidity;
    public boolean cloudy;

    @JsonFormat(pattern = "yyyy-MM-dd")
    public String date;

    public WeatherData() {}

    public WeatherData(String city, double temperature, int humidity, boolean cloudy, String date) {
        this.city = city;
        this.temperature = temperature;
        this.humidity = humidity;
        this.cloudy = cloudy;
        this.date = date;
    }
}

