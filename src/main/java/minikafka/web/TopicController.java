package minikafka.web;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import minikafka.Broker;
import minikafka.ProduceResult;
import minikafka.Record;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/topics")
@RequiredArgsConstructor
public class TopicController {
    private final Broker broker;

    public record ProduceResponse(int partition, long offset) {}
    public record MessageView(int partition, long offset, String value) {}
    public record TopicSummary(String topic, int partitions, long[] endOffsets) {}

    //GET /api/topics
    @GetMapping
    public List<TopicSummary> list() throws IOException
    {
        List<TopicSummary> out = new ArrayList<>();
        for (String name : broker.listTopics()) {
            long[] ends = broker.endOffsets(name);
            out.add(new TopicSummary(name, ends.length, ends));
        }
        return out;
    }

    //GET /api/topics/orders   -> partitions and how many records each holds
    @GetMapping("/{topic}")
    public TopicSummary describe(@PathVariable String topic)
    {
        long[] ends = broker.endOffsets(topic);
        return new TopicSummary(topic, ends.length, ends);
    }

    //POST /api/topics/orders/messages?key=alice      header X-Client-Id: order-service     body: deposit
    @PostMapping("/{topic}/messages")
    @ResponseStatus(HttpStatus.CREATED)
    public ProduceResponse produce(@PathVariable String topic, @RequestParam(defaultValue = "") String key, @RequestBody String body,
                                   @RequestHeader(value = Actors.HEADER, required = false) String clientId,
                                   HttpServletRequest request) throws IOException
    {
        ProduceResult r = broker.produce(Actors.rest(clientId, request), topic, key, body.getBytes(StandardCharsets.UTF_8));
        return new ProduceResponse(r.partition(), r.offset());
    }

    //GET /api/topics/orders/partitions/0/messages/3
    @GetMapping("/{topic}/partitions/{partition}/messages/{offset}")
    public ResponseEntity<String> fetch(@PathVariable String topic, @PathVariable int partition, @PathVariable long offset,
                                        @RequestHeader(value = Actors.HEADER, required = false) String clientId,
                                        HttpServletRequest request) throws IOException
    {
        return broker.fetch(Actors.rest(clientId, request), topic, partition, offset)
                .map(b -> ResponseEntity.ok(new String(b, StandardCharsets.UTF_8)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    //GET /api/topics/orders/partitions/0/messages?from=0&max=100   (you keep track of the offset yourself)
    @GetMapping("/{topic}/partitions/{partition}/messages")
    public List<MessageView> fetchBatch(@PathVariable String topic, @PathVariable int partition,
                                        @RequestParam(defaultValue = "0") long from, @RequestParam(defaultValue = "100") int max,
                                        @RequestHeader(value = Actors.HEADER, required = false) String clientId,
                                        HttpServletRequest request) throws IOException
    {
        List<MessageView> out = new ArrayList<>();
        for (Record r : broker.fetchBatch(Actors.rest(clientId, request), topic, partition, from, max))
            out.add(new MessageView(partition, r.offset(), new String(r.payload(), StandardCharsets.UTF_8)));
        return out;
    }
}
