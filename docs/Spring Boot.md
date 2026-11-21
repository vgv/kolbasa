# Spring Boot

`kolbasa-spring-boot-4-starter` sets kolbasa up in a Spring Boot application. You describe your queues in one bean, and
the starter creates their tables at startup, gives you `Producer`, `Consumer`, `Mutator` and `Inspector` beans, and
starts and stops your `QueueWorker`s together with the application. It also connects kolbasa to Spring Boot's metrics
and tracing.

It needs Spring Boot 4.0 or later.

## Table of Contents

1. [Adding the starter](#adding-the-starter)
2. [Standalone mode](#standalone-mode)
3. [Cluster mode](#cluster-mode)
4. [Both modes in one application](#both-modes-in-one-application)
5. [The schema](#the-schema)
6. [Using kolbasa while the application starts](#using-kolbasa-while-the-application-starts)
7. [Workers](#workers)
8. [Metrics and tracing](#metrics-and-tracing)
9. [Your own beans instead of the starter's](#your-own-beans-instead-of-the-starters)
10. [Properties](#properties)

---

## Adding the starter

```groovy
implementation "io.github.vgv:kolbasa-spring-boot-4-starter:$kolbasaVersion"
```

It brings `kolbasa` with it, so you don't need to add it separately.

On its own the starter does nothing. Kolbasa is switched on by a settings bean: `KolbasaStandaloneSettings`,
`KolbasaClusterSettings`, or both. Without one, the starter doesn't touch any database.

The two settings beans match kolbasa's two deployment modes, see
[From single node to cluster](Cluster%20architecture.md#from-single-node-to-cluster):

- **standalone**: the queues live in one database, the application's own;
- **cluster**: the queues are spread over several PostgreSQL nodes.

Spring Boot finds the starter by itself. To turn it off completely, exclude it:

```kotlin
@SpringBootApplication(exclude = [KolbasaAutoConfiguration::class])
```

---

## Standalone mode

In standalone mode the queues live in the application's own database: the `DataSource` that Spring Boot creates from
`spring.datasource.*`.

```kotlin
val ORDERS = Queue.of("orders", PredefinedDataTypes.String)
val EMAILS = Queue.of("emails", PredefinedDataTypes.String)

@Configuration
class KolbasaConfig {

    @Bean
    fun kolbasaStandaloneSettings() = KolbasaStandaloneSettings(
        queues = listOf(ORDERS, EMAILS),
        consumerOptions = ConsumerOptions(consumer = "billing")
    )
}
```

From Java, use the builder:

```java
@Bean
KolbasaStandaloneSettings kolbasaStandaloneSettings() {
    return KolbasaStandaloneSettings.builder(List.of(ORDERS, EMAILS))
        .consumerOptions(ConsumerOptions.builder().consumer("billing").build())
        .build();
}
```

With this bean the starter:

- creates or updates the tables of these queues at startup, see [The schema](#the-schema);
- creates four beans: `kolbasaStandaloneProducer`, `kolbasaStandaloneConsumer`, `kolbasaStandaloneMutator` and
  `kolbasaStandaloneInspector`, with the options from the settings.

Pass only main queues: the dead letter queue and the archive queue of a queue are created together with it.

Get the roles like any other bean, by their interfaces:

```kotlin
@Service
class OrderService(private val producer: Producer) {

    fun placeOrder(order: Order) {
        producer.send(ORDERS, order.toJson()).throwExceptionIfAny()
    }
}
```

The beans are a `DatabaseProducer`, a `DatabaseConsumer` and so on, but ask for the interface. Then a test can give
your service a fake of its own.

The application must have a `DataSource` bean. Without one, it doesn't start.

---

## Cluster mode

In cluster mode the queues are spread over several PostgreSQL nodes, see
[Cluster architecture.md](Cluster%20architecture.md).

```kotlin
@Configuration
class KolbasaConfig {

    @Bean(defaultCandidate = false)
    fun nodeA(): DataSource = HikariDataSource().apply {
        jdbcUrl = "jdbc:postgresql://node-a:5432/queues"
        // ...
    }

    @Bean(defaultCandidate = false)
    fun nodeB(): DataSource = HikariDataSource().apply {
        jdbcUrl = "jdbc:postgresql://node-b:5432/queues"
        // ...
    }

    @Bean
    fun kolbasaClusterSettings() = KolbasaClusterSettings(
        nodes = listOf(nodeA(), nodeB()),
        queues = listOf(ORDERS)
    )
}
```

With this bean the starter creates:

- `kolbasaCluster`, the `Cluster` over these nodes;
- `kolbasaClusterTopologyUpdater`. At startup it reads the cluster topology and creates the queues on every node. Then
  it reads the topology again every minute, until the application stops. If the first read fails, the application
  doesn't start: the cluster can't work without the topology;
- four beans: `kolbasaClusterProducer`, `kolbasaClusterConsumer`, `kolbasaClusterMutator` and
  `kolbasaClusterInspector`. You use them the same way as in standalone mode, by their interfaces.

**Mark the node `DataSource`s with `defaultCandidate = false`.** Spring Boot creates its own `DataSource` only when the
application has none. Without `defaultCandidate = false`, Boot sees your nodes and doesn't create its own. Then whatever
in your application needs the `DataSource` gets a node instead of your database, or, with two or more nodes, can't
choose one and fails. With `defaultCandidate = false`, a node is given only to those who ask for it by name.

`nodeA()` inside the same `@Configuration` class returns the bean, not a new `DataSource`. So the settings get the same
objects that Spring closes when the application stops. In Kotlin this needs the `kotlin("plugin.spring")` Gradle
plugin, which Spring Initializr adds to every Kotlin project.

Other fields of `KolbasaClusterSettings`:

- `nodes` and `queues` can be functions instead of lists. They are called again on every topology update, so a node or
  a queue added to the list is picked up without a restart:
  `KolbasaClusterSettings({ currentNodes() }, { listOf(ORDERS) })`.
- `producerOptions`, `consumerOptions` and `mutatorOptions`, the same as in standalone mode.
- `shardStrategy`: how the producer picks the shard of a message, and with it the node. By default,
  `ShardStrategy.ThreadLocalWithInterval()`.
- `topologyUpdaterOptions`: how often the topology is read again (every minute by default, from 1 second to
  15 minutes), on what executor, and what to do when a read fails.

From Java, use `KolbasaClusterSettings.builder(nodes, queues)`, the same as in standalone mode.

---

## Both modes in one application

An application can have both settings beans. Then it has two beans of every role, and every place that gets one must
say which, with `@KolbasaStandalone` or `@KolbasaCluster`:

```kotlin
@Service
class OrderService(
    @KolbasaStandalone private val outbox: Producer,
    @KolbasaCluster private val orders: Consumer
)
```

Without the annotation, Spring doesn't know which bean to pick, and the application doesn't start with
`NoUniqueBeanDefinitionException`. That's on purpose: neither mode is the default, so adding a cluster to an
application never rewires its existing code silently. An application with one settings bean doesn't need the
annotations at all.

### Recipe: the transactional outbox

The usual reason to have both modes is the
[transactional outbox](Patterns.md#the-transactional-outbox): a business write and a message that always agree. The
cluster is in other databases, so a send to it can't be part of your transaction. Instead, the message goes to a queue
in your own database, in the same transaction, and a pipe moves it to the cluster in the background.

```kotlin
val ORDERS = Queue.of("orders", PredefinedDataTypes.String) // in the cluster

// In your database: the same queue under another name, so the two can't drift apart. The options are its own: every
// receive of the pipe spends an attempt, also while the cluster is down, so it needs many
val ORDERS_OUTBOX = ORDERS.withName("orders_outbox").withOptions(QueueOptions(defaultAttempts = 10_000))

@Configuration
class KolbasaConfig {

    @Bean
    fun kolbasaStandaloneSettings() = KolbasaStandaloneSettings(queues = listOf(ORDERS_OUTBOX))

    @Bean
    fun kolbasaClusterSettings() = KolbasaClusterSettings(nodes = listOf(nodeA(), nodeB()), queues = listOf(ORDERS))

    // Moves the messages from the outbox to the cluster
    @Bean
    fun outboxRelay(@KolbasaStandalone consumer: Consumer, @KolbasaCluster producer: Producer) = QueueWorker(
        QueuePipe.asIs(PipeSource(ORDERS_OUTBOX, consumer), PipeTarget(ORDERS, producer))
    )

    // nodeA() and nodeB() as in cluster mode
}
```

The send uses the connection of your transaction:

```kotlin
@Service
class OrderService(private val dataSource: DataSource, private val jdbc: JdbcClient) {

    private val outbox = ConnectionAwareDatabaseProducer()

    @Transactional
    fun placeOrder(order: Order) {
        jdbc.sql("insert into orders(id, customer) values (?, ?)").params(order.id, order.customer).update()

        // The connection of the current transaction: if it rolls back, the message is gone too. Don't close it, it
        // belongs to the transaction. A failed send throws, and the insert is rolled back
        outbox.send(DataSourceUtils.getConnection(dataSource), ORDERS_OUTBOX, SendMessage(order.toJson()))
            .throwExceptionIfAny()
    }
}
```

The starter's `Producer` beans can't do this: each of their sends takes its own connection and runs in a transaction of
its own. `DataSourceUtils` works with Spring's JDBC and JPA transactions.

The pipe delivers at least once, so the consumer of `ORDERS` should be ready to see a message twice. The pipe takes a
message at most once a minute, the default visibility timeout, so 10 000 attempts outlive about a week of an outage.
The details are in the KDoc of `QueuePipe`, and in
[Moving messages to another server](../README.md#moving-messages-to-another-server).

`ORDERS` itself can't be the outbox. Its options would apply on both sides: the default delay twice, and its attempts,
five by default, would run out in about five minutes of an outage. And your database couldn't be a node of the
cluster, since the outbox and the cluster queue would be the same table.

---

## The schema

### Standalone mode

At startup the bean `kolbasaStandaloneSchemaInitializer` creates or updates the tables of the queues from
`KolbasaStandaloneSettings`. It is safe on every start: kolbasa compares the database with your queues and changes only
what differs, see [Schema generation and migration](Architecture.md#schema-generation-and-migration).

It runs after the other database initializers: Flyway, Liquibase and Spring Boot's `schema.sql`. So a migration can
prepare the database for kolbasa, but can't use kolbasa's tables. The recipes below show what to do then.

A failed statement gets one more try: two instances that start at the same time can race on the same statement, and
the second try finds the work done. If it fails again, the application doesn't start, and the exception lists the
failed statements.

`kolbasa.standalone.schema.enabled=false` switches the schema initializer off.

#### Recipe: grants on the queue tables

Say a `reporting` role should read every queue table. The tables don't exist yet when Flyway runs, but default
privileges cover tables created later. Put this into an ordinary migration:

```sql
alter default privileges grant select on tables to reporting;
```

Every table that kolbasa creates after it gets the grant: at this startup, and when you add a queue later.

Default privileges cover the tables created by the user that runs the statement. If Flyway and the application connect
as different users, name the application's user: `alter default privileges for role app grant select on tables to
reporting;`.

#### Recipe: Flyway after kolbasa

For anything else that needs kolbasa's tables, run one more Flyway after kolbasa, with its own folder and its own
history table:

```kotlin
@Bean
@DependsOn("kolbasaStandaloneSchemaInitializer")
fun afterKolbasaMigrations(dataSource: DataSource) = InitializingBean {
    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/after-kolbasa")
        .table("flyway_after_kolbasa_history")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .load()
        .migrate()
}
```

Three things matter here:

- **It is not a `Flyway` bean.** Spring Boot treats every `Flyway` bean as a database initializer, and kolbasa waits
  for all of them. A `Flyway` bean that waits for kolbasa makes a cycle, and the application doesn't start.
- **`baselineOnMigrate(true)`.** By then the database is not empty, and without it Flyway refuses to start a new history
  table.
- **`baselineVersion("0")`.** The default baseline is version 1, and Flyway skips the migrations up to the baseline.
  Without it, your `V1__…` is skipped, and nothing tells you.

#### Recipe: the schema is yours

To manage kolbasa's tables yourself, set `kolbasa.standalone.schema.enabled=false`. Then nothing creates them. Get the
DDL from `SchemaHelpers.generateCreateOrUpdateStatements` and put it into your migrations, see
[Schema generation and migration](Architecture.md#schema-generation-and-migration).

### Cluster mode

The cluster creates its schema itself, on every topology update: the queues from `KolbasaClusterSettings`, on every
node. A node or a queue added later gets its tables within one update interval.

`kolbasa.standalone.schema.enabled` doesn't apply here. An empty `queues` list means "the schema is mine": the cluster
creates no tables.

---

## Using kolbasa while the application starts

A bean of yours can use the starter's roles while it is created, in its constructor for example. Every role of the
starter waits for the schema in standalone mode, and for the first topology read in cluster mode, so the tables and the
topology are ready.

A role you build yourself doesn't wait. If a bean uses it while the application starts, make that bean depend on the
starter's:

```kotlin
@Bean
@DependsOn("kolbasaClusterTopologyUpdater")
fun auditProducer(cluster: Cluster) = ClusterProducer(cluster, ProducerOptions(producer = "audit"))
```

In standalone mode, depend on `kolbasaStandaloneSchemaInitializer`. Without it, a send while the application starts
fails: with `Cluster topology isn't initialized` in cluster mode, and with `relation "…" does not exist` in standalone
mode.

---

## Workers

Declare a `QueueWorker` bean, and the starter starts it once the application is up and stops it when the application
stops:

```kotlin
@Bean
fun emailWorker(consumer: Consumer) = QueueWorker(
    QueueProcessor(EMAILS, consumer) { message ->
        sendEmail(message.data)
        HandleResult.DONE
    }
)
```

A `QueueWorker` over a `QueuePipe` works the same way, as in the [outbox recipe](#recipe-the-transactional-outbox).

- **Workers start last.** They start after everything else, the web server too, so they don't take messages before
  the application is ready.
- **Workers stop first,** before the web server's graceful shutdown. Every worker is told to stop, and then the starter
  waits for each, up to its `QueueWorkerOptions.stopTimeout`, 30 seconds by default. A busy thread finishes its batch
  first. So the workers finish at the same time, not one after another.
- **Don't call `start()` yourself.** A worker starts only once, so the starter's call would fail, and the application
  wouldn't start.
- **`kolbasa.workers.auto-startup=false` leaves the workers stopped,** for example in tests. A test that needs a worker
  starts it by hand: `worker.start()`. Spring still stops it when the context closes.

---

## Metrics and tracing

The starter connects its role beans to the metrics and the tracing your application already has.

**Metrics.** With a `MeterRegistry` bean, the role beans report their metrics to it. Spring Boot makes one when the
application has `spring-boot-starter-actuator`. Add `micrometer-registry-prometheus` and open the endpoint with
`management.endpoints.web.exposure.include=health,prometheus`, and kolbasa's metrics are at `/actuator/prometheus`,
together with the rest. Every metric and tag is in [Metrics.md](Metrics.md).

**Tracing.** Add `kolbasa-opentelemetry`, with the same version as the starter:

```groovy
implementation "io.github.vgv:kolbasa-opentelemetry:$kolbasaVersion"
```

With it and an `OpenTelemetry` bean, the producer and consumer beans trace through that bean. Spring Boot makes one
when the application has `spring-boot-starter-opentelemetry`. A `QueueWorker` over the starter's consumer traces the
handling of every message too: its `QueueProcessor` takes the tracing from the consumer. The spans are described in the
KDoc of `OpenTelemetryTracing`. A mutator has metrics, but no tracing.

**How it works.** The starter makes two beans, `kolbasaMetricsConfig` over the `MeterRegistry` and `kolbasaTracing`
over the `OpenTelemetry`. It puts them into the options of its role beans, but only where `metricsConfig` or `tracing`
is not set:

- an option you set in `producerOptions`, `consumerOptions` or `mutatorOptions` stays as it is;
- a `MetricsConfig` or `Tracing` bean of yours replaces the starter's, for every role bean. `MetricsConfig.None` and
  `Tracing.NONE` take kolbasa out of the metrics or the traces, and the rest of the application keeps them;
- without these beans the options stay empty, and kolbasa uses `Kolbasa.metricsConfig` and `Kolbasa.tracing`, as it
  does without the starter. The starter never sets them.

A role you build yourself doesn't get them on its own. Take the beans and pass them in:

```kotlin
@Bean
@DependsOn("kolbasaClusterTopologyUpdater")
fun auditProducer(cluster: Cluster, metricsConfig: MetricsConfig?, tracing: Tracing?) = ClusterProducer(
    cluster,
    ProducerOptions(producer = "audit", metricsConfig = metricsConfig, tracing = tracing)
)
```

The `?` makes them optional: without a `MeterRegistry` or an `OpenTelemetry` bean, the parameter is `null`.

---

## Your own beans instead of the starter's

A role bean of yours replaces the starter's: declare a `Producer` bean, and the starter doesn't create its own. This is
also how a test gets a fake.

One exception keeps both modes working. A `ClusterProducer` bean doesn't replace the standalone producer, and a
`DatabaseProducer` bean doesn't replace the cluster one. The same goes for the other roles. So in an application with
both modes, declare your bean by its class, and mark it with the mode:

```kotlin
@Bean
@KolbasaStandalone
fun auditingProducer(dataSource: DataSource): DatabaseProducer = DatabaseProducer(dataSource, auditOptions)
```

A bean declared only as `Producer` replaces both producers of the starter.

The same goes for the `MetricsConfig` and `Tracing` beans, see [Metrics and tracing](#metrics-and-tracing).

`Cluster`, the topology updater and the schema initializer can't be replaced. The schema initializer can only be
switched off, with `kolbasa.standalone.schema.enabled=false`.

---

## Properties

| Property | Default | What it does |
|---|---|---|
| `kolbasa.standalone.schema.enabled` | `true` | Creates or updates the tables of the standalone queues at startup |
| `kolbasa.workers.auto-startup` | `true` | Starts the `QueueWorker` beans with the application |

Everything else — the queues, the nodes and the options — goes into the settings beans.
