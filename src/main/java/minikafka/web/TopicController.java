package minikafka.web;

import lombok.RequiredArgsConstructor;
import minikafka.Broker;
import minikafka.ProduceResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** HTTP front-end over the same Broker used by the TCP protocol. */
@RestController
@RequestMapping("/api/topics")
@RequiredArgsConstructor
public class TopicController {
    private final Broker broker;

    public record ProduceResponse(int partition, long offset) {}

    /** POST /api/topics/orders/messages?key=alice   (body = message text) */
    @PostMapping("/{topic}/messages")
    @ResponseStatus(HttpStatus.CREATED)
    public ProduceResponse produce(@PathVariable String topic,
                                   @RequestParam(defaultValue = "") String key,
                                   @RequestBody String body) throws IOException {
        ProduceResult r = broker.produce(topic, key, body.getBytes(StandardCharsets.UTF_8));
        return new ProduceResponse(r.partition(), r.offset());
    }

    /** GET /api/topics/orders/partitions/1/messages/0 -> 200 text, or 404 if not written yet */
    @GetMapping("/{topic}/partitions/{partition}/messages/{offset}")
    public ResponseEntity<String> fetch(@PathVariable String topic,
                                        @PathVariable int partition,
                                        @PathVariable long offset) throws IOException {
        return broker.fetch(topic, partition, offset)
                .map(b -> ResponseEntity.ok(new String(b, StandardCharsets.UTF_8)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @ExceptionHandler({IllegalArgumentException.class, ArrayIndexOutOfBoundsException.class})
    ProblemDetail badRequest(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
