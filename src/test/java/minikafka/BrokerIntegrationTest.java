package minikafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Boots the full Spring context (random TCP port, temp data dir) and tests TCP + REST. */
@SpringBootTest(properties = "minikafka.port=0")
@AutoConfigureMockMvc
class BrokerIntegrationTest {
    @TempDir static Path dataDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("minikafka.data-dir", () -> dataDir.toString());
    }

    @Autowired Broker broker;
    @Autowired MockMvc mvc;

    @Test
    void endToEndOverTcp() throws Exception {
        assertThat(broker.isRunning()).isTrue();
        try (var c = new Client("localhost", broker.getPort())) {
            String first = c.produce("tcp-orders", "alice", "deposit");
            String second = c.produce("tcp-orders", "alice", "withdraw");
            String[] f = first.split(":"), s = second.split(":");
            assertThat(f[0]).isEqualTo(s[0]);                                        // same partition
            assertThat(Long.parseLong(s[1])).isEqualTo(Long.parseLong(f[1]) + 1);    // offsets increase
            assertThat(c.fetch("tcp-orders", Integer.parseInt(f[0]), Long.parseLong(f[1]))).isEqualTo("deposit");
            assertThat(c.fetch("tcp-orders", Integer.parseInt(s[0]), Long.parseLong(s[1]))).isEqualTo("withdraw");
            assertThat(c.fetch("tcp-orders", Integer.parseInt(s[0]), 99)).isNull();  // EMPTY
        }
    }

    @Test
    void restProduceThenFetchOverRestAndTcp() throws Exception {
        mvc.perform(post("/api/topics/rest-orders/messages").param("key", "bob")
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.offset").value(0));

        int partition = Math.floorMod("bob".hashCode(), 4);
        mvc.perform(get("/api/topics/rest-orders/partitions/{p}/messages/0", partition))
                .andExpect(status().isOk())
                .andExpect(content().string("hello"));
        mvc.perform(get("/api/topics/rest-orders/partitions/{p}/messages/99", partition))
                .andExpect(status().isNotFound());

        try (var c = new Client("localhost", broker.getPort())) {
            assertThat(c.fetch("rest-orders", partition, 0)).isEqualTo("hello");
        }
    }

    @Test
    void rejectsUnsafeTopicNames() throws Exception {
        mvc.perform(post("/api/topics/{t}/messages", "..%2Fevil")
                        .contentType(MediaType.TEXT_PLAIN).content("x"))
                .andExpect(status().is4xxClientError());
        try (var c = new Client("localhost", broker.getPort())) {
            org.junit.jupiter.api.Assertions.assertThrows(java.io.IOException.class,
                    () -> c.produce("../evil", "k", "v"));
        }
    }
}
