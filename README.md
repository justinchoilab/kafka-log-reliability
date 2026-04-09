# kafka-log-reliability

Kafka를 쓸 때와 직접 DB에 적재할 때의 신뢰성 차이를 눈으로 확인하는 데모입니다.  
Producer 3개가 동일한 이벤트를 **두 경로로 동시에** 저장하고, Dashboard에서 각 경로의 성공/실패/복구 현황을 실시간으로 비교합니다.

## 핵심 개념: Direct vs Kafka

Producer는 매 tick마다 같은 이벤트를 두 경로로 처리합니다.

| | Direct 경로 | Kafka 경로 |
|---|---|---|
| **방식** | Producer가 Redis에 직접 `HSET` | Producer → Kafka → Consumer → Redis |
| **DB 장애 시** | 즉시 실패 (재시도 없음) | Consumer가 in-memory 큐에 보관, DB 복구 후 순차 처리 |
| **Kafka 장애 시** | 영향 없음 | Producer가 Redis에 직접 fallback 저장 |
| **특징** | 빠르고 단순, 장애에 취약 | 지연 있음, 장애 격리·복구 가능 |

### DB 장애 시나리오

Dashboard에서 **Redis DB 장애**를 켜면:

- **Direct**: `fault:db` 키를 확인하고 Redis 쓰기를 건너뜀 → 즉시 실패 카운트 증가
- **Kafka**: Producer는 Kafka에 정상 발행. Consumer는 메시지를 받지만 DB가 살아날 때까지 in-memory 큐에 쌓아둠. DB 복구 신호(`fault:events` 채널의 `db:off`)를 받으면 큐에 있던 메시지를 순서대로 처리 → **유실 없이 복구**

### Kafka 장애 시나리오

Dashboard에서 **Kafka 브로커 장애**를 켜면:

- **Direct**: 영향 없음, 계속 정상 적재
- **Kafka**: Producer가 Kafka 전송 실패를 감지하고 Redis에 직접 fallback 저장 (`source: 'fallback'`)

## 구성

| 서비스 | 포트 | 역할 |
|--------|------|------|
| `producer-1/2/3` | 3001 | 100ms 간격 이벤트 발행 |
| `consumer` | 3002 | Kafka 구독 → Redis 저장 → Dashboard 전송 |
| `dashboard` | **8080** | 실시간 통계 및 장애 시뮬레이션 UI |
| `kafka` | 9092 | KRaft 단일 브로커 (Apache Kafka 3.8.1) |
| `redis` | 6379 | 저장소 + 장애 시그널 채널 |

## 시작하기

```bash
docker compose up --build
```

Dashboard: http://localhost:8080

## 환경 변수

`.env`는 리포지토리에 포함되어 있습니다. `docker-compose.yml`의 Kafka 클러스터 ID에 사용됩니다.

| 변수 | 설명 |
|------|------|
| `KAFKA_CLUSTER_ID` | Kafka KRaft 클러스터 ID |
