package minikafka.web;

import lombok.RequiredArgsConstructor;
import minikafka.Broker;
import minikafka.cluster.ClusterStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cluster")
@RequiredArgsConstructor
public class ClusterController {
    private final Broker broker;

    //GET /api/cluster -> this node's role (STANDALONE / FOLLOWER / CANDIDATE / LEADER), term, leader and peers
    @GetMapping
    public ClusterStatus status()
    {
        return broker.clusterStatus();
    }
}
