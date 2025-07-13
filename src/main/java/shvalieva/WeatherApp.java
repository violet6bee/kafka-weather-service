package shvalieva;

public class WeatherApp {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Использование: --generate или --read <город>");
            return;
        }

        switch (args[0]) {
            case "--generate":
                new WeatherProducer().generateAndSend();
                break;
            case "--read":
                if (args.length < 2) {
                    System.out.println("Укажите город для анализа, пример: --read Москва");
                    return;
                }
                new WeatherConsumer().readAndAnalyze(args[1]);
                break;
            default:
                System.out.println("Неизвестный аргумент: " + args[0]);
        }
    }
}

