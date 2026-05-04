# KoreForge.Kafka.Vue — Specification

**Status**: Draft v0.1 — design only, implementation not started.

## 1. Overview

`KoreForge.Kafka.Vue` is a Vue 3 component library for visualising and lightly administering Kafka clusters that an application interacts with. It targets read-only operational telemetry (consumer lag, topic metadata, partition assignments) — not destructive admin (no topic creation, no offset reset, no delete).

It is consumed by:

- The [`KoreForge.Monitoring.Shell`](../../KoreForge.Monitoring.Shell/doc/specification.md) (registered as the `kafkaConsumer`, `kafkaLag`, and `kafkaTopic` capability components).
- Any internal ops UI built on `KoreForge.Kafka.AdminClient` (NuGet) that wants drop-in panels.

### Design Goals

1. **Read-only by default** — Components do not mutate broker state. Mutating operations are out of scope for v1.
2. **Backed by `IKafkaAdminClient`** — All data flows through the existing `KoreForge.Kafka.AdminClient` HTTP façade exposed by the application's monitoring endpoints. The Vue layer never speaks AdminClient protocol directly.
3. **Cluster-agnostic** — A single component can be pointed at any monitored cluster by changing its endpoint prop.
4. **Composable** — Headless composables underlie every component, identical pattern to `@koreforge/vue-scripts` and `@koreforge/jex-vue`.
5. **Real-time aware** — Lag and partition assignments stream over the monitoring SignalR hub.

### Package Identity

| Field | Value |
|-------|-------|
| NPM Name | `@koreforge/kafka-vue` |
| Repository | `eco-web/KoreForge.Kafka.Vue` |
| License | MIT |
| Vue Version | `^3.5.0` |
| Build Tool | Vite (library mode) |
| Backend | `KoreForge.Kafka` + `KoreForge.Kafka.AdminClient` (NuGet), surfaced via `KoreForge.Monitoring.AspNetCore` |

## 2. Architecture

```
@koreforge/kafka-vue
├── composables/
│   ├── useKafkaTopicMetadata     ← GET /monitoring/kafka/topics/{name}
│   ├── useKafkaConsumerLag       ← GET /monitoring/kafka/consumer-groups/{group}/lag
│   ├── useKafkaConsumerStream    ← SignalR subscription to ConsumerGroupLagChanged
│   └── useKafkaCluster           ← Aggregates topic + group + broker metadata
├── components/
│   ├── KafkaConsumerPanel        ← Default consumer-group dashboard
│   ├── KafkaLagPanel             ← Compact lag chart (per partition + per topic)
│   ├── KafkaTopicPanel           ← Topic metadata viewer (partitions, replicas, ISR)
│   ├── KafkaPartitionMap         ← Visual partition→broker assignment map
│   └── KafkaConsumerHealthBadge  ← Tiny inline health indicator (green/amber/red)
└── theme/
    └── default.css
```

### 2.1 Two-Layer Design

Identical pattern to `@koreforge/vue-scripts`:

- **Composables** expose reactive state + actions, no DOM.
- **Components** wire composables into a default UI.

```vue
<KafkaConsumerPanel
  endpoint="/monitoring/kafka"
  consumerGroup="payments-consumer"
  :topics="['payments']"
  :pollMs="5000"
/>
```

```typescript
import { useKafkaConsumerLag } from '@koreforge/kafka-vue'

const { lag, totalLag, byPartition, refresh } = useKafkaConsumerLag({
  endpoint: '/monitoring/kafka',
  consumerGroup: 'payments-consumer',
  topicFilter: ['payments']
})
```

### 2.2 Relation to KoreForge.Kafka

The Vue layer does not bundle a Kafka client. The backend application registers a `KoreForge.Kafka.AdminClient` instance and exposes a small read-only HTTP façade through its `KoreForge.Monitoring.AspNetCore` capability provider. Vue calls only that façade.

This means:

- The browser never holds Kafka credentials.
- All admin authorisation (which clusters/topics this user can see) is enforced server-side.
- Adding a new Kafka cluster to the UI requires no Vue changes — just a new application registration.

## 3. Components

### 3.1 KafkaConsumerPanel

Composite dashboard for a single consumer group.

**Props:**

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `endpoint` | `string` | required | Monitoring base URL (e.g. `/monitoring/kafka`) |
| `consumerGroup` | `string` | required | Group id |
| `topics` | `string[]` | `[]` | Restrict view to these topics; empty = all subscribed |
| `pollMs` | `number` | `5000` | Snapshot poll interval (deltas via SignalR are always live) |
| `compact` | `boolean` | `false` | Compact layout (no charts, just totals) |

**Layout:**

```
┌──────────────────────────────────────────────────────────────┐
│ payments-consumer                       Healthy   Lag: 4,201 │
├──────────────────────────────────────────────────────────────┤
│ Members (3)                Topics (1)                        │
│  consumer-1  → P0,P3,P6     payments  Partitions 12          │
│  consumer-2  → P1,P4,P7                                      │
│  consumer-3  → P2,P5,P8                                      │
├──────────────────────────────────────────────────────────────┤
│ Lag per partition (chart)                                    │
└──────────────────────────────────────────────────────────────┘
```

### 3.2 KafkaLagPanel

Pure lag visualiser. Used both standalone and as the metric overlay on a `PipelineDiagram` `kafkaConsumer` node.

| Prop | Type | Description |
|------|------|-------------|
| `endpoint` | `string` | Monitoring base URL |
| `consumerGroup` | `string` | Group id |
| `topic` | `string?` | Restrict to one topic |
| `mode` | `'chart' \| 'sparkline' \| 'numeric'` | Default `chart` |

### 3.3 KafkaTopicPanel

Topic metadata: partition count, replica counts, in-sync replica count, leader broker per partition, total/used disk where available.

### 3.4 KafkaPartitionMap

Renders a partition × broker grid showing leadership and ISR membership at a glance. Useful for spotting imbalance or under-replicated partitions.

### 3.5 KafkaConsumerHealthBadge

Inline badge: green / amber / red driven by a configurable lag-threshold prop. Designed for cell-level use inside other tables.

## 4. Backend Contracts Consumed

The application's monitoring layer must expose this read-only façade (planned default implementation in `KoreForge.Kafka.Monitoring`):

| Endpoint | Returns | Source |
|---|---|---|
| `GET  {endpoint}/topics` | List of topic names | `IKafkaAdminClient.GetTopicMetadataAsync` (cached) |
| `GET  {endpoint}/topics/{name}` | `TopicMetadataDto` (partitions, replicas, leaders) | same |
| `GET  {endpoint}/consumer-groups` | List of consumer group ids | `IKafkaAdminClient` |
| `GET  {endpoint}/consumer-groups/{group}/lag` | `ConsumerGroupLagSnapshotDto` | `GetConsumerGroupLagSummaryAsync` |
| `GET  {endpoint}/consumer-groups/{group}/members` | `ConsumerGroupMemberDto[]` | `IKafkaAdminClient` |
| SignalR `/monitoring/hub` | `ConsumerGroupLagChanged`, `PartitionAssignmentChanged` events | `KoreForge.Monitoring.SignalR` push from background poller |

Mutating operations (offset commit/reset, topic create, etc.) are explicitly **not** exposed for v1.

## 5. DTOs

```typescript
export interface TopicMetadataDto {
  name: string
  partitionCount: number
  replicationFactor: number
  partitions: TopicPartitionDto[]
}

export interface TopicPartitionDto {
  partition: number
  leader: number              // broker id
  replicas: number[]
  isr: number[]               // in-sync replicas
}

export interface ConsumerGroupLagSnapshotDto {
  group: string
  totalLag: number
  byTopic: TopicLagDto[]
  capturedAtUtc: string
}

export interface TopicLagDto {
  topic: string
  totalLag: number
  byPartition: PartitionLagDto[]
}

export interface PartitionLagDto {
  partition: number
  current: number
  end: number
  lag: number
}

export interface ConsumerGroupMemberDto {
  memberId: string
  clientId: string
  host: string
  assignments: TopicPartitionAssignmentDto[]
}

export interface TopicPartitionAssignmentDto {
  topic: string
  partitions: number[]
}
```

These mirror records in `KoreForge.Kafka.Contracts` (planned NuGet package).

## 6. Theming

Inherits `--kf-*` tokens from `@koreforge/vue-scripts`. Adds:

```css
--kf-kafka-leader-bg:        var(--kf-color-primary);
--kf-kafka-replica-bg:       var(--kf-color-surface-alt);
--kf-kafka-out-of-sync-bg:   var(--kf-color-warning);
--kf-kafka-lag-low:          var(--kf-color-success);
--kf-kafka-lag-medium:       var(--kf-color-warning);
--kf-kafka-lag-high:         var(--kf-color-error);
```

## 7. Build & Distribution

Same Vite library-mode build as the other `@koreforge/*-vue` packages. Externals:

- `vue`
- `@microsoft/signalr`
- A charting library (chart.js or apexcharts; final choice deferred to implementation).

Published as `@koreforge/kafka-vue`. CI tag pattern: `KoreForge.Kafka.Vue/v*`.

## 8. Non-Goals (v1)

- Topic / partition / acl creation or deletion.
- Offset commit or reset.
- Consumer-group assignment override.
- Schema-registry integration.
- Direct Kafka protocol from the browser.
