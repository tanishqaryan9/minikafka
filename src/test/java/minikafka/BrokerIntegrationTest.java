package minikafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

//Boots the REAL Spring application (web server, controllers, configuration binding, lifecycle) and talks to it over HTTP and TCP.
//Uses only java.net.http, so it does not depend on the Spring Boot test-support packages that moved between Boot versions.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = "minikafka.port=0")
class BrokerIntegrationTest {
    @TempDir static Path dir;
    static final int HTTP_PORT = freePort();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("server.port", () -> HTTP_PORT);
        r.add("minikafka.data-dir", () -> dir.resolve("data").toString());
        r.add("minikafka.audit.file", () -> dir.resolve("logs").resolve("audit.log").toString());
        r.add("logging.file.name", () -> dir.resolve("logs").resolve("app.log").toString());
    }

    @Autowired Broker broker;
    final HttpClient http = HttpClient.newHttpClient();

    static int freePort() {
        try (var s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    HttpResponse<String> call(String method, String path, String body, String clientId) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + HTTP_PORT + path));
        if (body == null) b.method(method, HttpRequest.BodyPublishers.noBody());
        else b.method(method, HttpRequest.BodyPublishers.ofString(body)).header("Content-Type", "text/plain");
        if (clientId != null) b.header("X-Client-Id", clientId);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void restProduceThenFetchOverRestAndTcp() throws Exception {
        var created = call("POST", "/api/topics/it-orders/messages?key=bob", "hello", "order-service");
        assertThat(created.statusCode()).isEqualTo(201);
        assertThat(created.body()).contains("\"offset\":0");

        int partition = Math.floorMod("bob".hashCode(), 4);
        var ok = call("GET", "/api/topics/it-orders/partitions/" + partition + "/messages/0", null, "dashboard");
        assertThat(ok.statusCode()).isEqualTo(200);
        assertThat(ok.body()).isEqualTo("hello");
        assertThat(call("GET", "/api/topics/it-orders/partitions/" + partition + "/messages/99", null, null).statusCode()).isEqualTo(404);

        try (var c = new Client("localhost", broker.getPort(), "tcp-reader")) {
            assertThat(c.fetch("it-orders", partition, 0)).isEqualTo("hello");     // REST wrote it, TCP reads it
        }
        var batch = call("GET", "/api/topics/it-orders/partitions/" + partition + "/messages?from=0&max=10", null, null);
        assertThat(batch.statusCode()).isEqualTo(200);
        assertThat(batch.body()).contains("\"value\":\"hello\"");
    }

    @Test
    void badRequestsGetProperStatusCodes() throws Exception {
        assertThat(call("POST", "/api/topics/bad!name/messages?key=x", "v", null).statusCode()).isEqualTo(400);
        assertThat(call("GET", "/api/topics/it-orders/partitions/9/messages/0", null, null).statusCode()).isEqualTo(400);
        assertThat(call("GET", "/api/topics/it-orders/partitions/0/messages/abc", null, null).statusCode()).isEqualTo(400);
        assertThat(call("POST", "/api/topics/it-orders/messages?key=x", null, null).statusCode()).isEqualTo(400);        // body required
        assertThat(call("POST", "/api/topics/__consumer_offsets/messages?key=x", "v", null).statusCode()).isEqualTo(400);
    }

    @Test
    void consumerGroupOverRest() throws Exception {
        for (int i = 0; i < 8; i++)
            assertThat(call("POST", "/api/topics/it-jobs/messages?key=k" + i, "job-" + i, "producer").statusCode()).isEqualTo(201);

        assertThat(call("GET", "/api/groups/workers/topics/it-jobs/poll?consumer=w1", null, null).statusCode()).isEqualTo(409);   // not joined yet

        var join = call("POST", "/api/groups/workers/topics/it-jobs/join?consumer=w1", null, null);
        assertThat(join.statusCode()).isEqualTo(200);
        assertThat(join.body()).contains("\"partitions\":[0,1,2,3]");

        var poll = call("GET", "/api/groups/workers/topics/it-jobs/poll?consumer=w1&max=100", null, null);
        assertThat(poll.statusCode()).isEqualTo(200);
        assertThat(poll.body()).contains("job-0").contains("job-7");

        var commit = call("POST", "/api/groups/workers/topics/it-jobs/commit?consumer=w1", null, null);
        assertThat(commit.statusCode()).isEqualTo(200);

        var describe = call("GET", "/api/groups/workers", null, null);
        assertThat(describe.statusCode()).isEqualTo(200);
        assertThat(describe.body()).contains("\"group\":\"workers\"").contains("\"consumer\":\"w1\"");

        assertThat(call("POST", "/api/groups/workers/topics/it-jobs/leave?consumer=w1", null, null).statusCode()).isEqualTo(200);
        assertThat(call("POST", "/api/groups/workers/topics/it-jobs/commit?consumer=w1&partition=0", null, null).statusCode()).isEqualTo(400);   // offset missing
    }

    @Test
    void auditTrailIsServedOverHttpAndWrittenToItsOwnFile() throws Exception {
        call("POST", "/api/topics/it-audit/messages?key=alice", "deposit", "order-service");
        call("POST", "/api/groups/auditors/topics/it-audit/join?consumer=a1", null, "billing-a1");
        call("GET", "/api/groups/auditors/topics/it-audit/poll?consumer=a1", null, "billing-a1");

        var produced = call("GET", "/api/audit?type=PRODUCE&actor=order-service&topic=it-audit", null, null);
        assertThat(produced.statusCode()).isEqualTo(200);
        assertThat(produced.body()).contains("\"type\":\"PRODUCE\"").contains("rest:order-service@").contains("\"topic\":\"it-audit\"");

        var consumed = call("GET", "/api/audit?type=CONSUME&group=auditors", null, null);
        assertThat(consumed.body()).contains("rest:billing-a1@").contains("\"consumer\":\"a1\"").contains("deposit");

        var info = call("GET", "/api/audit/info", null, null);
        assertThat(info.body()).contains("\"enabled\":true").contains("audit.log");

        String file = Files.readString(dir.resolve("logs").resolve("audit.log"));         // the separate file on disk
        assertThat(file).contains(" PRODUCE actor=rest:order-service@").contains("topic=it-audit");
        assertThat(file).contains(" CONSUME actor=rest:billing-a1@").contains("group=auditors");
    }

    @Test
    void topicsClusterAndHealthEndpoints() throws Exception {
        call("POST", "/api/topics/it-listing/messages?key=k", "v", null);
        var topics = call("GET", "/api/topics", null, null);
        assertThat(topics.statusCode()).isEqualTo(200);
        assertThat(topics.body()).contains("\"topic\":\"it-listing\"");
        assertThat(call("GET", "/api/topics/it-listing", null, null).body()).contains("\"partitions\":4");

        assertThat(call("GET", "/api/cluster", null, null).body()).contains("\"role\":\"STANDALONE\"");
        assertThat(call("GET", "/actuator/health", null, null).body()).contains("UP");
    }
}
