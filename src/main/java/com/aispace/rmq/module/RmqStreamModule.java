package com.aispace.rmq.module;

import com.rabbitmq.client.DeliverCallback;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author kangmoo Heo
 */
@Slf4j
public class RmqStreamModule extends RmqModule {
    private static final String STREAM_OFFSET = "x-stream-offset";
    private int qos = 100;

    private Object streamOffset = "next";

    public RmqStreamModule(String host, String userName, String password, int bufferCount) {
        super(host, userName, password, bufferCount);
    }

    public RmqStreamModule(String host, String userName, String password, int port, int bufferCount) {
        super(host, userName, password, port, bufferCount);
    }

    public RmqStreamModule(String host, String userName, String password, int port, int qos, String streamOffset) {
        super(host, userName, password, port);
        this.qos = qos;
        this.streamOffset = streamOffset;
    }

    public RmqStreamModule(String host, String userName, String password, int port, int qos, int streamOffset) {
        super(host, userName, password, port);
        this.qos = qos;
        this.streamOffset = streamOffset;
    }

    public RmqStreamModule(String host, String userName, String password, int port, int qos, Date streamOffset) {
        super(host, userName, password, port);
        this.qos = qos;
        this.streamOffset = streamOffset;
    }

    @Override
    public void connect(Runnable onConnected, Runnable onDisconnected) throws IOException, TimeoutException {
        super.connect(onConnected, onDisconnected);
        channel.basicQos(qos); // QoS must be specified in RMQ Stream
    }

    @Override
    public void queueDeclare(String queueName) throws IOException {
        super.queueDeclare(queueName, Map.of("x-queue-type", "stream"));
    }

    /**
     * 지정된 이름의 메시지 스트림 큐를 생성한다.
     *
     * @param queueName      생성할 큐의 이름
     * @param maxLengthBytes 큐의 최대 총 크기(바이트)
     * @param maxAge         메시지 수명. 가능한 단위: Y, M, D, h, m, s. (e.g. 7D = 일주일)
     * @throws IOException 큐 생성에 실패한 경우
     */
    public void queueDeclare(String queueName, long maxLengthBytes, @NonNull String maxAge) throws IOException {
        super.queueDeclare(queueName, Map.of("x-queue-type", "stream",
                "x-max-length-bytes", maxLengthBytes,
                "x-max-age", maxAge));
    }

    @Override
    public void registerConsumer(String queueName, DeliverCallback deliverCallback, Map<String, Object> arguments) throws IOException {
        if (arguments == null) {
            arguments = Map.of(STREAM_OFFSET, streamOffset);
        }
        channel.basicConsume(queueName, false, arguments, deliverCallback, consumerTag -> {
        });
    }

    /**
     * @param streamOffset first - 로그에서 사용 가능한 첫 번째 메시지부터 시작
     *                     last - 메시지의 마지막으로 작성된 "청크"에서 읽기 시작
     *                     next - 오프셋을 지정하지 않은 것과 동일
     */
    public void registerConsumer(String queueName, DeliverCallback deliverCallback, String streamOffset) throws IOException {
        this.registerConsumer(queueName, deliverCallback, Map.of(STREAM_OFFSET, streamOffset));
    }

    /**
     * @param streamOffset 로그에서 오프셋의 메시지부터 시작
     */
    public void registerConsumer(String queueName, DeliverCallback deliverCallback, int streamOffset) throws IOException {
        this.registerConsumer(queueName, deliverCallback, Map.of(STREAM_OFFSET, streamOffset));
    }

    /**
     * @param streamOffset 로그에 첨부할 시점을 지정하는 타임스탬프 값
     */
    public void registerConsumer(String queueName, DeliverCallback deliverCallback, Date streamOffset) throws IOException {
        this.registerConsumer(queueName, deliverCallback, Map.of(STREAM_OFFSET, streamOffset));
    }
}
