package minikafka.config;

import minikafka.audit.AuditLog;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AuditConfig {
    //Plain Java class, so it is created here; Spring calls close() on shutdown
    @Bean(destroyMethod = "close")
    public AuditLog auditLog(BrokerProperties props) {
        BrokerProperties.Audit a = props.getAudit();
        if (!a.isEnabled()) return AuditLog.disabled();
        return new AuditLog(a.getFile(), props.getNodeId(), a.getMaxFileSizeMb() * 1024L * 1024L,
                a.getMaxBackups(), a.getPayloadPreview(), a.isConsumePerRecord(), a.getMemoryEvents());
    }
}
