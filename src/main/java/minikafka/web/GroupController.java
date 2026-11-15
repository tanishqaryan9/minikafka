package minikafka.web;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import minikafka.Broker;
import minikafka.group.GroupCoordinator;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

//Consumer groups over HTTP. A "consumer" is any name you choose, for example billing-1.
//The X-Client-Id header is optional here: without it the audit log uses the consumer name as the actor.
@RestController
@RequestMapping("/api/groups")
@RequiredArgsConstructor
public class GroupController {
    private final Broker broker;

    public record JoinView(int generation, List<Integer> partitions) {}
    public record HeartbeatView(int generation) {}
    public record RecordView(int partition, long offset, String value) {}
    public record PollView(int generation, List<RecordView> records) {}

    private static String actor(String clientId, String consumer, HttpServletRequest request) {
        return Actors.rest(clientId == null || clientId.isBlank() ? consumer : clientId, request);
    }

    //POST /api/groups/billing/topics/orders/join?consumer=billing-1
    @PostMapping("/{group}/topics/{topic}/join")
    public JoinView join(@PathVariable String group, @PathVariable String topic, @RequestParam String consumer,
                         @RequestHeader(value = Actors.HEADER, required = false) String clientId,
                         HttpServletRequest request) throws IOException
    {
        GroupCoordinator.JoinResult r = broker.joinGroup(actor(clientId, consumer, request), group, topic, consumer);
        return new JoinView(r.generation(), r.partitions());
    }

    @PostMapping("/{group}/topics/{topic}/leave")
    public void leave(@PathVariable String group, @PathVariable String topic, @RequestParam String consumer,
                      @RequestHeader(value = Actors.HEADER, required = false) String clientId,
                      HttpServletRequest request)
    {
        broker.leaveGroup(actor(clientId, consumer, request), group, topic, consumer);
    }

    @PostMapping("/{group}/topics/{topic}/heartbeat")
    public HeartbeatView heartbeat(@PathVariable String group, @PathVariable String topic, @RequestParam String consumer)
    {
        return new HeartbeatView(broker.heartbeat(group, topic, consumer));
    }

    //GET /api/groups/billing/topics/orders/poll?consumer=billing-1&max=10
    @GetMapping("/{group}/topics/{topic}/poll")
    public PollView poll(@PathVariable String group, @PathVariable String topic, @RequestParam String consumer,
                         @RequestParam(defaultValue = "10") int max,
                         @RequestHeader(value = Actors.HEADER, required = false) String clientId,
                         HttpServletRequest request) throws IOException
    {
        GroupCoordinator.PollResult r = broker.poll(actor(clientId, consumer, request), group, topic, consumer, max);
        List<RecordView> out = new ArrayList<>();
        for (GroupCoordinator.PolledRecord p : r.records())
            out.add(new RecordView(p.partition(), p.offset(), new String(p.payload(), StandardCharsets.UTF_8)));
        return new PollView(r.generation(), out);
    }

    //POST /api/groups/billing/topics/orders/commit?consumer=billing-1              commits the current positions
    //POST /api/groups/billing/topics/orders/commit?consumer=billing-1&partition=0&offset=5   commits one partition
    @PostMapping("/{group}/topics/{topic}/commit")
    public Map<Integer, Long> commit(@PathVariable String group, @PathVariable String topic, @RequestParam String consumer,
                                     @RequestParam(required = false) Integer partition, @RequestParam(required = false) Long offset,
                                     @RequestHeader(value = Actors.HEADER, required = false) String clientId,
                                     HttpServletRequest request) throws IOException
    {
        if ((partition == null) != (offset == null))
            throw new IllegalArgumentException("give both partition and offset, or neither");
        Map<Integer, Long> explicit = null;
        if (partition != null) {
            explicit = new TreeMap<>();
            explicit.put(partition, offset);
        }
        return new TreeMap<>(broker.commit(actor(clientId, consumer, request), group, topic, consumer, explicit));
    }

    //GET /api/groups/billing   -> members, who owns which partition, committed offset, position and lag
    @GetMapping("/{group}")
    public GroupCoordinator.GroupInfo describe(@PathVariable String group) throws IOException
    {
        return broker.describeGroup(group);
    }
}
