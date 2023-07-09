package com.aispace.rmq.module;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author kangmoo Heo
 */
@Slf4j
public class RmqStreamService {
    private Environment environment;
    private Producer producer;
    private Consumer consumer;
    private final String host;
    private final String username;
    private final String password;
    private final Integer port;

    public RmqStreamService(String host, String username, String password, Integer port) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.port = port;
    }

    public void connect(String streamName) {
        environment = Environment.builder()
                .host(host)
                .username(username)
                .password(password)
                .port(port).build();
        producer = environment.producerBuilder().stream(streamName).build();
    }

    public void connect(String streamName, java.util.function.Consumer<byte[]> onMsgRecv) {
        connect(streamName);
        consumer = environment.consumerBuilder().stream(streamName)
                .messageHandler((offset, message) -> {
                    try {
                        onMsgRecv.accept(message.getBodyAsBinary());
                    } catch (Exception e) {
                        log.warn("Err Occurs", e);
                    }
                }).build();
    }

    public void sendMessage(String message) {
        this.sendMessage(message.getBytes());
    }

    public void sendMessage(byte[] message) {
        producer.send(producer.messageBuilder()
                .addData(message)
                .build(), confirmationStatus -> {
        });
    }

    public void close() {
        for (AutoCloseable closable : List.of(environment, producer, consumer)) {
            try {
                if (closable != null) {
                    closable.close();
                }
            } catch (Exception e) {
                log.warn("Err Occurs while closing", e);
            }
        }
    }
}
