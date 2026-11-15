package minikafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedBufferTest {
    @Test
    void deliversAllItemsThroughSizeFourBuffer() throws Exception {
        var buf = new BoundedBuffer<Integer>(4);
        long[] sum = {0};
        var consumer = new Thread(() -> {
            try { for (int i = 0; i < 1000; i++) sum[0] += buf.take(); }
            catch (InterruptedException ignored) {}
        });
        consumer.start();
        for (int i = 1; i <= 1000; i++) buf.put(i);
        consumer.join();
        assertThat(sum[0]).isEqualTo(500_500);
    }
}
