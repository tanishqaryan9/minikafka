package minikafka.web;

import lombok.RequiredArgsConstructor;
import minikafka.Broker;
import minikafka.audit.AuditLog;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

//Read the audit trail without opening the file: what was added, what was consumed, by whom
@RestController
@RequestMapping("/api/audit")
@RequiredArgsConstructor
public class AuditController {
    private final Broker broker;

    public record AuditInfo(boolean enabled, String file, long sizeBytes) {}

    //GET /api/audit?limit=50&type=CONSUME&actor=billing&topic=orders&group=billing   (all filters optional)
    @GetMapping
    public List<AuditLog.Event> events(@RequestParam(defaultValue = "100") int limit,
                                       @RequestParam(required = false) String type,
                                       @RequestParam(required = false) String actor,
                                       @RequestParam(required = false) String topic,
                                       @RequestParam(required = false) String group)
    {
        return broker.audit().recent(limit, type, actor, topic, group);
    }

    @GetMapping("/info")
    public AuditInfo info()
    {
        AuditLog a = broker.audit();
        return new AuditInfo(a.isEnabled(), a.isEnabled() ? a.file().toAbsolutePath().toString() : null, a.fileSize());
    }
}
