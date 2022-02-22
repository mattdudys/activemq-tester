package com.dudys.activemqtester;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.SimpleMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.*;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static int prefetchSize;

    private static boolean useAsyncSend;

    private static boolean sessionTransacted;

    private static boolean deliveryPersistent;

    private static int sessionAcknowledgeMode;

    private static String queueName;

    private static int concurrentConsumers;

    private static String brokerUrl;

    private static String userName;

    private static String password;

    private static int n;

    private static boolean concurrentProduceConsume;

    public static void main(String args[]) throws IllegalAccessException {
        brokerUrl = System.getProperty("brokerUrl", "tcp://localhost:61616");
        queueName = System.getProperty("queueName", "test_" + UUID.randomUUID().toString().replace("-", ""));
        n = Integer.parseInt(System.getProperty("n", "100000"));

        prefetchSize = Integer.parseInt(System.getProperty("prefetchSize", "2000"));
        useAsyncSend = Boolean.parseBoolean(System.getProperty("useAsyncSend", "true"));
        sessionTransacted = Boolean.parseBoolean(System.getProperty("sessionTransacted", "false"));
        deliveryPersistent = Boolean.parseBoolean(System.getProperty("deliveryPersistent", "true"));
        sessionAcknowledgeMode = Integer.parseInt(System.getProperty("sessionAcknowledgeMode", "3")); // DUPS_OK
        concurrentConsumers = Integer.parseInt(System.getProperty("concurrentConsumers", "1"));
        userName = System.getProperty("userName", null);
        password = System.getProperty("password", null);
        concurrentProduceConsume = Boolean.parseBoolean(System.getProperty("concurrentProduceConsume", "false"));

        for (Field f : Main.class.getDeclaredFields()) {
            LOGGER.info("{}: {}", f.getName(), f.get(null));
        }

        final ConnectionFactory cf = setupConnectionFactory();
        if (concurrentProduceConsume) {
            LOGGER.info("producing and consuming concurrently");
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            Future<?> publishFuture = executorService.submit(() -> publish(cf, n));
            Future<?> consumeFuture = executorService.submit(() -> consume(cf, n));
            try {
                publishFuture.get();
                consumeFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.warn("Interrupted while waiting for publish and consume tasks to finish.", e);
                publishFuture.cancel(true);
                consumeFuture.cancel(true);
            }
        } else {
            LOGGER.info("producing then consuming serially.");
            publish(cf, n);
            consume(cf, n);
        }
        System.exit(0);
    }

    private static ConnectionFactory setupConnectionFactory() {
        final ActiveMQPrefetchPolicy pfp = new ActiveMQPrefetchPolicy();
        pfp.setQueuePrefetch(prefetchSize);

        final ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        cf.setUseAsyncSend(useAsyncSend);
        cf.setPrefetchPolicy(pfp);
        if (userName != null) {
            cf.setUserName(userName);
        }
        if (password != null) {
            cf.setPassword(password);
        }
        final PooledConnectionFactory pcf = new PooledConnectionFactory(cf);
        pcf.setMaxConnections(2);

        return pcf;
    }

    private static void publish(final ConnectionFactory cf, int n) {
        final JmsTemplate jmsTemplate = new JmsTemplate(cf);
        jmsTemplate.setSessionTransacted(sessionTransacted);
        jmsTemplate.setSessionAcknowledgeMode(sessionAcknowledgeMode);

        if (!deliveryPersistent) {
            jmsTemplate.setExplicitQosEnabled(true);
            jmsTemplate.setDeliveryPersistent(false);
        }

        LOGGER.info("publish start");
        long start = System.currentTimeMillis();
        for (int i = n; i > 0; i--) {
            final int j = i - 1;
            jmsTemplate.convertAndSend(queueName, String.format("%s bottles of beer on the wall, %s bottles of beer! Take one down, pass it around, %s bottles of beer on the wall.", i, i, j));
            if (i % 5000 == 0) {
                LOGGER.info("Remaining: {}", i);
            }
        }
        LOGGER.info("publish end");
        printSummary(start, System.currentTimeMillis(), n);
    }

    private static class Delegate {

        private final CountDownLatch latch;

        public Delegate(final CountDownLatch latch) {
             this.latch = latch;
        }

        public void handleMessage(String s) {
            latch.countDown();
            if (latch.getCount() % 5000 == 0) {
                LOGGER.info("Remaining: {}", latch.getCount());
            }
        }
    }

    private static void consume2(final ConnectionFactory cf, int n) throws JMSException {
        final CountDownLatch latch = new CountDownLatch(n);
        final Connection c = cf.createConnection();
        c.start();
        final Session s = c.createSession(sessionTransacted, sessionAcknowledgeMode);
        final Queue q = s.createQueue(queueName);
        final MessageConsumer mc = s.createConsumer(q);
        mc.setMessageListener(message -> {
            latch.countDown();
            if (latch.getCount() % 5000 == 0) {
                LOGGER.info("Remaining: {}", latch.getCount());
            }
        });

        LOGGER.info("consume start");
        final long start = System.currentTimeMillis();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted while waiting for consumer to finish", e);
        }
        final long end = System.currentTimeMillis();
        LOGGER.info("consume done");
        mc.close();
        s.close();
        c.stop();
        c.close();
        printSummary(start, end, n);
    }

    private static void consume(final ConnectionFactory cf, int n) {
        final CountDownLatch latch = new CountDownLatch(n);

        final MessageListenerAdapter mla = new MessageListenerAdapter();
        mla.setDelegate(new Delegate(latch));
        mla.setDefaultListenerMethod("handleMessage");

        final SimpleMessageListenerContainer smlc = new SimpleMessageListenerContainer();
        smlc.setConnectionFactory(cf);
        smlc.setConcurrentConsumers(concurrentConsumers);
        smlc.setSessionAcknowledgeMode(sessionAcknowledgeMode);
        smlc.setDestinationName(queueName);
        smlc.setSessionTransacted(sessionTransacted);
        smlc.setMessageListener(mla);

        LOGGER.info("consume start");
        final long start = System.currentTimeMillis();
        smlc.start();
        try {
            latch.await();
        } catch (final InterruptedException e) {
            LOGGER.warn("interrupted when waiting for consumer to finish.", e);
        }
        final long end = System.currentTimeMillis();
        LOGGER.info("consume done");
        smlc.shutdown();
        printSummary(start, end, n);
    }

    private static void printSummary(long start, long end, int n) {
        double diff = (end - start) / 1000.0;
        double rate = n / diff;
        LOGGER.info("Duration: {} seconds", diff);
        LOGGER.info("Rate: {} per second", rate);
    }

}
