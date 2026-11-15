package minikafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TopicTest {
    @TempDir Path tmp;

    @Test
    void sameKeyMapsToSamePartitionAndStaysInRange() throws Exception {
        try (var topic = new Topic(tmp, "orders", 4)) {
            assertThat(topic.partitionFor("alice")).isEqualTo(topic.partitionFor("alice"));
            for (String k : List.of("alice", "bob", "polygenelubricants", "", "zzzzzz"))
                assertThat(topic.partitionFor(k)).isBetween(0, 3);
        }
    }
}
