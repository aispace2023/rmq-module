package com.aispace.rmq.module;

import com.rabbitmq.client.*;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * RabbitMQ와의 연결을 관리하며, 다양한 RabbitMQ 관련 작업을 제공하는 클래스.
 * 이 클래스는 다음과 같은 기능을 수행한다:
 * <ul>
 *   <li> RabbitMQ 서버와의 연결 설정 및 종료 </li>
 *   <li> 채널 생성 </li>
 *   <li> 메시지 큐 선언 </li>
 *   <li> 지정된 큐에 메시지 전송 </li>
 *   <li> 지정된 큐에 소비자 등록 </li>
 * </ul>
 */
@Slf4j
@Getter
public class RmqService implements AutoCloseable{
    // RabbitMQ 서버와의 연결을 재시도하는 간격(단위:ms)
    private static final int RECOVERY_INTERVAL = 1000;
    // RabbitMQ 서버에게 전송하는 heartbeat 요청의 간격(단위:sec)
    private static final int REQUESTED_HEARTBEAT = 5;
    // RabbitMQ 서버와 연결을 시도하는 최대 시간(단위:ms)
    private static final int CONNECTION_TIMEOUT = 2000;

    private final String host;
    private final String userName;
    private final String password;
    private final SSLContext sslContext;

    // RabbitMQ 서버와의 연결과 채널을 관리하기 위한 변수
    private Connection connection;
    private Channel channel;

    /**
     * RabbitMQ 서버와의 SSL 연결을 설정하는 생성자
     *
     * @param host       RabbitMQ 서버의 호스트
     * @param userName   RabbitMQ 서버에 연결할 사용자 이름
     * @param password   해당 사용자의 비밀번호
     * @param sslContext SSL 연결을 위한 SSLContext (null이면 SSL을 사용하지 않는다)
     */
    public RmqService(String host, String userName, String password, SSLContext sslContext) {
        this.host = host;
        this.userName = userName;
        this.password = password;
        this.sslContext = sslContext;
    }

    public RmqService(String host, String userName, String password) {
        this(host, userName, password, null);
    }

    @Synchronized
    public void connect() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        // RabbitMQ 서버 정보 설정
        factory.setHost(host);
        factory.setUsername(userName);
        factory.setPassword(password);

        // 자동 복구를 활성화하고, 네트워크 복구 간격, heartbeat 요청 간격, 연결 타임아웃 시간을 설정
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(RECOVERY_INTERVAL);
        factory.setRequestedHeartbeat(REQUESTED_HEARTBEAT);
        factory.setConnectionTimeout(CONNECTION_TIMEOUT);

        // SSL 설정 (있는 경우)
        if (sslContext != null) {
            factory.useSslProtocol(sslContext);
        }

        // 연결과 채널 생성 시도
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();

        // 블로킹 리스너 추가
        this.connection.addBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) {
                log.error("RabbitMQ connection is now blocked, reason: " + reason);
            }

            @Override
            public void handleUnblocked() {
                log.info("RabbitMQ connection is unblocked");
            }
        });

    }

    /**
     * 지정된 이름의 메시지 큐를 생성한다.
     *
     * @param queueName 생성할 큐의 이름
     * @throws IOException 큐 생성에 실패한 경우
     */
    @Synchronized
    public void queueDeclare(String queueName) throws IOException {
        channel.queueDeclare(queueName, false, false, false, null);
    }

    /**
     * 지정된 큐에 문자열 형태의 메시지를 처리할 소비자를 등록한다.
     *
     * @param queueName 메시지를 전송할 큐의 이름
     * @param message   전송할 메시지
     * @throws IOException 메시지 전송에 실패한 경우
     */
    @Synchronized
    public void sendMessage(String queueName, String message) throws IOException {
        channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        log.info("Sent message: {} to queue: {}", message, queueName);
    }

    /**
     * 지정된 큐에 바이트 배열 형태의 메시지를 처리할 소비자를 등록한다.
     *
     * @param queueName 메시지를 전송할 큐의 이름
     * @param message   전송할 메시지 (바이트 배열)
     * @throws IOException 메시지 전송에 실패한 경우
     */
    @Synchronized
    public void sendMessage(String queueName, byte[] message) throws IOException {
        channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message);
        log.info("Sent message to queue: {}", queueName);
    }


    /**
     * 지정된 큐에 소비자를 등록한다.
     *
     * @param queueName       소비자를 등록할 큐의 이름
     * @param deliverCallback 메시지가 수신될 때 호출되는 콜백
     * @throws IOException 소비자 등록에 실패한 경우
     */
    @Synchronized
    public void registerConsumer(String queueName, DeliverCallback deliverCallback) throws IOException {
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    /**
     * 지정된 큐에 문자열 형태의 메시지를 처리할 소비자를 등록한다.
     * 주로 텍스트 메시지를 받고 처리하는 경우에 사용한다.
     *
     * @param queueName   소비자를 등록할 큐의 이름
     * @param msgCallback 메시지가 수신될 때 호출되는 콜백, 메시지는 문자열로 전달됨
     * @throws IOException 소비자 등록에 실패한 경우
     */
    public void registerStringConsumer(String queueName, Consumer<String> msgCallback) throws IOException {
        registerConsumer(queueName, (s, delivery) -> msgCallback.accept(new String(delivery.getBody(), StandardCharsets.UTF_8)));
    }

    /**
     * 지정된 큐에 바이트 배열 형태의 메시지를 처리할 소비자를 등록한다.
     * 주로 바이너리 데이터를 받고 처리하는 경우에 사용한다.
     *
     * @param queueName   소비자를 등록할 큐의 이름
     * @param msgCallback 메시지가 수신될 때 호출되는 콜백, 메시지는 바이트 배열로 전달됨
     * @throws IOException 소비자 등록에 실패한 경우
     */
    public void registerByteConsumer(String queueName, Consumer<byte[]> msgCallback) throws IOException {
        registerConsumer(queueName, (s, delivery) -> msgCallback.accept(delivery.getBody()));
    }


    /**
     * RabbitMQ 서버와의 연결 및 채널을 종료한다.
     * 연결이나 채널 종료 과정에서 오류가 발생하면 로그에 출력하고 해당 예외를 던진다.
     */
    @Override
    public void close() {
        Exception exception = null;

        try {
            if (this.channel != null && this.channel.isOpen()) {
                this.channel.close();
            }
        } catch (Exception e) {
            log.error("Error while closing the channel: ", e);
            exception = e;
        }

        try {
            if (this.connection != null && this.connection.isOpen()) {
                this.connection.close();
            }
        } catch (Exception e) {
            log.error("Error while closing the connection: ", e);
            if (exception == null) {
                exception = e;
            } else {
                exception.addSuppressed(e);
            }
        }

        if (exception != null) {
            throw new RuntimeException(exception);
        }
    }
}