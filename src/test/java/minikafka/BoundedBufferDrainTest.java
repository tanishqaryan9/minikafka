package minikafka;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedBufferDrainTest {
    @Test
    void drainToMovesWhatIsThereWithoutBlocking() throws Exception {
        var buf = new BoundedBuffer<Integer>(8);
        for (int i = 1; i <= 5; i++) buf.put(i);
        List<Integer> out = new ArrayList<>();
        assertThat(buf.drainTo(out, 3)).isEqualTo(3);
        assertThat(out).containsExactly(1, 2, 3);
        assertThat(buf.size()).isEqualTo(2);
        assertThat(buf.drainTo(out, 10)).isEqualTo(2);
        assertThat(buf.drainTo(out, 10)).isEqualTo(0);                       // empty: returns at once
        assertThat(out).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void drainFreesSlotsForBlockedProducers() throws Exception {
        var buf = new BoundedBuffer<Integer>(2);
        buf.put(1);
        buf.put(2);
        Thread producer = new Thread(() -> {
            try { buf.put(3); } catch (InterruptedException ignored) { }
        });
        producer.start();
        Thread.sleep(100);
        assertThat(producer.isAlive()).isTrue();                              // full: producer is waiting
        buf.drainTo(new ArrayList<>(), 1);
        producer.join(2000);
        assertThat(producer.isAlive()).isFalse();                             // a slot opened, producer went through
        assertThat(buf.size()).isEqualTo(2);
    }
}
