package minikafka.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;


@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "minikafka")
public class BrokerProperties {
    //TCP port of the framed-protocol broker. 0 picks a free port
    @Min(0) @Max(65535)
    private int port = 9092;

    @NotNull
    private Path dataDir = Path.of("data");

    //Partitions created for every new topic
    @Min(1)
    private int partitions = 4;

    //Size of the connection-handler thread pool
    @Min(1)
    private int workerThreads = 16;
}
