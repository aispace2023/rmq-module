package com.aispace.rmq.module;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.DefaultExceptionHandler;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
public class RmqModule {
    private ScheduledExecutorService scheduler;

    // RabbitMQ 서버와의 연결을 재시도하는 간격(단위:ms)
    public static final int RECOVERY_INTERVAL = 1000;
    // RabbitMQ 서버에게 전송하는 heartbeat 요청의 간격(단위:sec)
    public static final int REQUESTED_HEARTBEAT = 5;
    // RabbitMQ 서버와 연결을 시도하는 최대 시간(단위:ms)
    public static final int CONNECTION_TIMEOUT = 2000;

    protected final String host;
    protected final String userName;
    protected final String password;
    protected final Integer port;

    // RabbitMQ 서버와의 연결과 채널을 관리하기 위한 변수
    protected Connection connection;
    protected Channel channel;

    /**
     * @param host     RabbitMQ 서버의 호스트
     * @param userName RabbitMQ 서버에 연결할 사용자 이름
     * @param password 해당 사용자의 비밀번호
     */
    public RmqModule(String host, String userName, String password) {
        this.host = host;
        this.userName = userName;
        this.password = password;
        this.port = null;
    }

    public RmqModule(String host, String userName, String password, int port) {
        this.host = host;
        this.userName = userName;
        this.password = password;
        this.port = port;
    }

    /**
     * RabbitMQ 서버에 연결을 수립하며 통신을 위한 채널을 생성하고, 연결에 대한 예외 처리기를 설정하고, 만약 연결이 복구 가능한 경우, 복구 리스너도 설정한다.
     * 연결과 채널이 성공적으로 수립되면 제공된 onConnected 콜백을, 연결에 실패하면 onDisconnected 콜백을 호출한다.
     *
     * @param onConnected    연결과 채널이 성공적으로 수립되었을 때 호출되는 콜백함수
     * @param onDisconnected 예기치 않은 연결 드라이버 예외가 발생했을 때 호출되는 콜백
     * @throws IOException      연결과 채널을 생성하는 동안 I/O 에러가 발생한 경우
     * @throws TimeoutException 연결과 채널을 생성하는 동안 타임아웃이 발생한 경우
     */
    public void connect(Runnable onConnected, Runnable onDisconnected) throws IOException, TimeoutException {
        if (isConnected()) {
            log.warn("RMQ Already Connected");
            return;
        }

        try {
            ConnectionFactory factory = new ConnectionFactory();

            // RabbitMQ 서버 정보 설정
            factory.setHost(host);
            factory.setUsername(userName);
            factory.setPassword(password);
            if (this.port != null) {
                factory.setPort(this.port);
            }

            // 자동 복구를 활성화하고, 네트워크 복구 간격, heartbeat 요청 간격, 연결 타임아웃 시간을 설정
            factory.setAutomaticRecoveryEnabled(true);
            factory.setNetworkRecoveryInterval(RECOVERY_INTERVAL);
            factory.setRequestedHeartbeat(REQUESTED_HEARTBEAT);
            factory.setConnectionTimeout(CONNECTION_TIMEOUT);

            factory.setExceptionHandler(new DefaultExceptionHandler() {
                @Override
                public void handleUnexpectedConnectionDriverException(Connection con, Throwable exception) {
                    super.handleUnexpectedConnectionDriverException(con, exception);
                    onDisconnected.run();
                }
            });

            // 연결과 채널 생성 시도
            this.connection = factory.newConnection();
            ((Recoverable) connection).addRecoveryListener(new RecoveryListener() {
                public void handleRecovery(Recoverable r) {
                    onConnected.run();
                }

                public void handleRecoveryStarted(Recoverable r) {
                    onDisconnected.run();
                }
            });

            this.channel = connection.createChannel();
            onConnected.run();
        } catch (Exception e) {
            onDisconnected.run();
            throw e;
        }
    }

    /**
     * 비동기적으로 RabbitMQ 서버에 연결을 시도하고, 연결이 실패할 경우 1초 후에 재시도한다.
     * 연결이 성공하면 onConnected 콜백을 호출하고, 연결 실패 시 onDisconnected 콜백을 호출한다.
     * 본 메서드는 스레드 안전하게 동작한다.
     *
     * @param onConnected    연결과 채널이 성공적으로 수립되었을 때 호출되는 콜백
     * @param onDisconnected 연결 시도가 실패했을 때 호출되는 콜백
     */
    @Synchronized
    public void connectWithAsyncRetry(Runnable onConnected, Runnable onDisconnected) {
        try {
            if (this.scheduler == null || this.scheduler.isShutdown()) {
                this.scheduler = Executors.newSingleThreadScheduledExecutor();
            }
            connect(onConnected, onDisconnected);
            this.scheduler.shutdown();
        } catch (Exception e) {
            log.warn("Err Occurs while RMQ Connection", e);
            close();
            scheduler.schedule(() -> connectWithAsyncRetry(onConnected, onDisconnected), 1000, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 지정된 이름의 메시지 큐를 생성한다.
     *
     * @param queueName 생성할 큐의 이름
     * @throws IOException 큐 생성에 실패한 경우
     */
    public void queueDeclare(String queueName) throws IOException {
        this.queueDeclare(queueName, null);
    }

    @Synchronized
    public void queueDeclare(String queueName, Map<String, Object> arguments) throws IOException {
        channel.queueDeclare(queueName, true, false, false, arguments);
    }

    /**
     * 지정된 이름의 큐에 메시지를 전송한다.
     *
     * @param queueName 전송할 큐의 이름
     * @param message   전송할 메시지
     * @throws IOException 메시지 전송에 실패한 경우
     */
    @Synchronized
    public void sendMessage(String queueName, String message) throws IOException {
        channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
    }

    /**
     * 지정된 이름의 큐에 만료 시간과 함께 메시지를 전송한다.
     *
     * @param queueName  전송할 큐의 이름
     * @param message    전송할 메시지
     * @param expiration 메시지의 만료 시간
     * @throws IOException 메시지 전송에 실패한 경우
     */
    @Synchronized
    public void sendMessage(String queueName, String message, int expiration) throws IOException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().expiration(Integer.toString(expiration)).build();
        channel.basicPublish("", queueName, properties, message.getBytes());
    }


    /**
     * 지정된 이름의 큐에 바이트 배열 형태의 메시지를 전송한다.
     *
     * @param queueName 전송할 큐의 이름
     * @param message   전송할 메시지의 바이트 배열
     * @throws IOException 메시지 전송에 실패한 경우
     */
    @Synchronized
    public void sendMessage(String queueName, byte[] message) throws IOException {
        channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message);
    }

    /**
     * 지정된 이름의 큐에 만료 시간과 함께 바이트 배열 형태의 메시지를 전송한다.
     *
     * @param queueName  전송할 큐의 이름
     * @param message    전송할 메시지의 바이트 배열
     * @param expiration 메시지의 만료 시간
     * @throws IOException 메시지 전송에 실패한 경우
     */
    @Synchronized
    public void sendMessage(String queueName, byte[] message, int expiration) throws IOException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().expiration(Integer.toString(expiration)).build();
        channel.basicPublish("", queueName, properties, message);
    }

    /**
     * 지정된 큐에 소비자를 등록한다.
     *
     * @param queueName       소비자를 등록할 큐의 이름
     * @param deliverCallback 메시지가 수신될 때 호출되는 콜백
     * @throws IOException 소비자 등록에 실패한 경우
     */
    @Synchronized
    public void registerConsumer(String queueName, DeliverCallback deliverCallback, Map<String, Object> arguments) throws IOException {
        channel.basicConsume(queueName, true, arguments, deliverCallback, consumerTag -> {
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
        registerConsumer(queueName, (s, delivery) -> msgCallback.accept(new String(delivery.getBody(), StandardCharsets.UTF_8)), null);
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
        registerConsumer(queueName, (s, delivery) -> msgCallback.accept(delivery.getBody()), null);
    }

    public boolean isConnected() {
        return this.channel != null && this.channel.isOpen() &&
                this.connection != null && this.connection.isOpen();
    }


    /**
     * RabbitMQ 서버와의 연결 및 채널을 종료한다.
     * 연결이나 채널 종료 과정에서 오류가 발생하면 로그에 출력한다.
     */
    @Synchronized
    public void close() {
        try {
            if (this.channel != null && this.channel.isOpen()) {
                this.channel.close();
            }
        } catch (Exception e) {
            log.warn("Error while closing the channel: ", e);
        }

        try {
            if (this.connection != null && this.connection.isOpen()) {
                this.connection.close();
            }
        } catch (Exception e) {
            log.warn("Error while closing the connection: ", e);
        }
    }
}