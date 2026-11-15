package minikafka.cli;

import minikafka.Client;

import java.util.ArrayList;
import java.util.List;

//Walks through a realistic session against a RUNNING broker, so the audit log fills with something to read:
//two named producers, two consumers in one group, a plain reader.
//
//  java -cp target/classes minikafka.cli.Demo [host] [port]        (defaults: localhost 9092)
//
//Then look at logs/audit.log (or GET http://localhost:8080/api/audit) to see who added and who consumed what.
public final class Demo {
    private Demo() {}

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;
        String topic = "demo-orders-" + (System.currentTimeMillis() % 100000);   // fresh topic every run
        System.out.println("Using topic " + topic + " on " + host + ":" + port + "\n");

        try (Client orders = new Client(host, port, "order-service");
             Client payments = new Client(host, port, "payment-service")) {
            System.out.println("== two producers add records ==");
            for (int i = 1; i <= 6; i++) System.out.println("order-service   -> " + orders.produce(topic, "customer-" + i, "order #" + i));
            for (int i = 1; i <= 4; i++) System.out.println("payment-service -> " + payments.produce(topic, "customer-" + i, "payment for order #" + i));
        }

        try (Client c1 = new Client(host, port, "billing-c1");
             Client c2 = new Client(host, port, "billing-c2")) {
            System.out.println("\n== two consumers join group 'billing' ==");
            c1.join("billing", topic, "c1");
            c2.join("billing", topic, "c2");
            System.out.println("c1 owns partitions " + c1.join("billing", topic, "c1").partitions());
            System.out.println("c2 owns partitions " + c2.join("billing", topic, "c2").partitions());

            System.out.println("\n== they poll (each only sees its own partitions) ==");
            List<String> seen = new ArrayList<>();
            for (int round = 0; round < 3; round++) {
                for (var m : c1.poll("billing", topic, "c1", 5).messages()) seen.add("c1 <- p" + m.partition() + "@" + m.offset() + " " + m.value());
                for (var m : c2.poll("billing", topic, "c2", 5).messages()) seen.add("c2 <- p" + m.partition() + "@" + m.offset() + " " + m.value());
            }
            seen.forEach(System.out::println);

            System.out.println("\n== they commit: the group remembers where it stopped, even across restarts ==");
            System.out.println("c1 committed " + c1.commit("billing", topic, "c1"));
            System.out.println("c2 committed " + c2.commit("billing", topic, "c2"));

            c1.leave("billing", topic, "c1");
            c2.leave("billing", topic, "c2");
        }

        try (Client dash = new Client(host, port, "audit-dashboard")) {
            System.out.println("\n== a plain reader (no group) fetches one record ==");
            for (int p = 0; p < 4; p++) {
                String v = dash.fetch(topic, p, 0);
                if (v != null) { System.out.println("audit-dashboard <- p" + p + "@0 " + v); break; }
            }
        }
        System.out.println("\nDone. Now read the audit trail:  tail -n 40 logs/audit.log   (or GET /api/audit)");
    }
}
