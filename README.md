# RmqModule 사용법

- `RmqModule`은 RabbitMQ와의 연결 및 관련 작업을 관리하는 클래스로, 이 모듈을 이용하여, RabbitMQ 서버와의 연결 설정, 메시지 큐 선언, 메시지 전송 및 수신 등의 작업을 수행할 수 있다.

## 시작하기 전에

- 먼저, 이 모듈을 사용하기 위해서는 RabbitMQ 서버가 설치되어 있어야 하며, 연결 정보 (호스트, 사용자 이름, 비밀번호)를 알고 있어야 한다.

## 1. RmqModule 인스턴스 생성

```java
RmqModule rmq = new RmqModule("{HOST}", "{USER_NAME}", "{PASSWORD}");
```

포트 번호도 함께 지정하려면 다음과 같이 사용합니다:

```java
RmqModule rmq = new RmqModule("{HOST}", "{USER_NAME}", "{PASSWORD}", {PORT});
```

## 2. RabbitMQ 서버에 연결

- 동기적 연결
```java
rmq.connect(onConnectedCallback, onDisconnectedCallback);
```

- 비동기적 연결 (연결 실패 시 재시도)

```java
rmq.connectWithAsyncRetry(onConnectedCallback, onDisconnectedCallback);
```

## 3. 큐 선언

- 기본 큐 선언

```java
rmq.queueDeclare("{QUEUE_NAME}");
```

- 추가 설정이 필요한 경우

```java
Map<String, Object> arguments = new HashMap<>();
// 여기에 필요한 설정 추가
rmq.queueDeclare("{QUEUE_NAME}", arguments);
```

> - 큐에 대한 다양한 속성들을 설정할 수 있다. 예를 들면, 메시지의 TTL(Time-To-Live), 큐의 최대 길이 등과 같은 설정이 포함된다.
> ``` java
> Map<String, Object> arguments = new HashMap<>();
> arguments.put("x-message-ttl", 60000); // 메시지 TTL 설정 (60초)
> arguments.put("x-queue-max-length", 1000); // 큐의 최대 길이 설정
> rmq.queueDeclare("{QUEUE_NAME}", arguments);
> ```

## 4. 메시지 전송

- 텍스트 메시지 전송

```java
rmq.sendMessage("{QUEUE_NAME}", "{MESSAGE}");
```

- 만료 시간을 가진 메시지 전송

```java
rmq.sendMessage("{QUEUE_NAME}", "{MESSAGE}", {EXPIRATION_TIME_IN_MS});
```

- 바이트 배열 메시지 전송

```java
rmq.sendMessage("{QUEUE_NAME}", {BYTE_ARRAY});
```

- 바이트 배열 메시지를 만료 시간과 함께 전송

```java
rmq.sendMessage("{QUEUE_NAME}", {BYTE_ARRAY}, {EXPIRATION_TIME_IN_MS});
```

## 5. 소비자 등록

- 문자열 메시지를 받는 소비자

```java
rmq.registerStringConsumer("{QUEUE_NAME}", message -> {
    // 여기에서 메시지 처리
});
```

- 바이트 배열 메시지를 받는 소비자

```java
rmq.registerByteConsumer("{QUEUE_NAME}", byteArrayMessage -> {
    // 여기에서 메시지 처리
});
```

## 6. 연결 확인9

```java
boolean isConnected = rmq.isConnected();
```

## 7. 연결 종료

```java
rmq.close();
```

## 주의사항

- 연결을 종료하기 전에 모든 작업이 완료되었는지 확인해야한다.
- 연결, 큐 선언, 메시지 전송 및 수신 등의 작업 중 오류가 발생할 수 있으므로 적절한 예외 처리가 필요하다.
- RabbitMQ 서버의 상태나 설정, 네트워크 상황에 따라 연결 및 작업 수행에 실패할 수 있다.