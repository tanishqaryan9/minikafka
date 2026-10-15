package minikafka;

import minikafka.config.BrokerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(BrokerProperties.class)
public class MiniKafkaApplication {
    public static void main(String[] args) {
        SpringApplication.run(MiniKafkaApplication.class, args);
    }
}
