# Rapport de TP : Architecture pilotée par les événements avec Kafka, Spring Cloud Stream et Kafka Streams



## 1. Objectifs

- Comprendre les bases d'une architecture pilotée par les événements (Event Driven Architecture).
- Mettre en place un broker **Apache Kafka** avec **Zookeeper**, en local puis avec **Docker**.
- Tester l'envoi et la réception de messages avec `kafka-console-producer` et `kafka-console-consumer`.
- Développer avec **Spring Cloud Stream** :
    - un **producer** via un Rest Controller,
    - un **consumer**,
    - un **supplier** (producteur automatique de flux),
    - un service de **Real Time Stream Processing** avec **Kafka Streams**,
    - une **application web** qui affiche les résultats en temps réel.

**Technologies :** Java 21, Spring Boot, Spring Cloud Stream, Spring for Apache Kafka, Kafka Streams, Docker / Docker Compose, Smoothie.js.

---

## 2. Partie 1 : Démarrer Kafka sans Docker

Étapes :

1. Télécharger Kafka depuis le site officiel et le décompresser.
2. Démarrer **Zookeeper** (dossier `bin/windows` avec les fichiers `.bat` sous Windows, dossier `bin` avec les fichiers `.sh` sous Linux / macOS).
3. Démarrer le serveur **Kafka** (broker).
4. Tester avec `kafka-console-producer` et `kafka-console-consumer`.

```bash
# Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka broker
bin/kafka-server-start.sh config/server.properties

# Consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic R2

# Producer
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic R2
```

---

## 3. Partie 2 : Démarrer Kafka avec Docker

Référence : <https://developer.confluent.io/quickstart/kafka-docker/>

### 3.1 Fichier `docker-compose.yml`

```yaml
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:7.3.0
    hostname: broker
    container_name: broker
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

### 3.2 Démarrage des conteneurs

```bash
docker compose up -d
```

![img.png](img.png)

Les deux conteneurs `zookeeper` et `broker` sont démarrés (vérification possible avec `docker ps`).

### 3.3 Test avec le consumer console

On lance un consumer qui s'abonne au topic `R2` à l'intérieur du conteneur `broker` :

```bash
docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic R2
```

![img_1.png](img_1.png)

### 3.4 Test avec le producer console

Dans un second terminal, on lance un producer sur le même topic `R2`, puis on saisit des messages (par exemple `hello`) :

```bash
docker exec --interactive --tty broker kafka-console-producer --bootstrap-server broker:9092 --topic R2
```

![img_2.png](img_2.png)

Les messages saisis côté producer apparaissent côté consumer. Le léger décalage observé vient du mécanisme de **polling** du consumer.

### 3.5 Consommer depuis le début du topic

Par défaut, un consumer ne reçoit que les nouveaux messages. L'option `--from-beginning` permet de relire tous les messages déjà présents dans le topic :

```bash
docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic R2 --from-beginning
```

![img_3.png](img_3.png)

---

## 4. Partie 3 : Spring Cloud Stream et Kafka Streams

### 4.1 Création du projet

Projet Maven généré avec **Spring Initializr** (Java 21), avec les dépendances :

- Spring Web
- Spring for Apache Kafka
- Spring for Apache Kafka Streams
- Cloud Stream (avec les binders `spring-cloud-stream-binder-kafka` et `spring-cloud-stream-binder-kafka-streams`)

> Spring Cloud Stream rend le code **indépendant du broker** utilisé (Kafka, RabbitMQ, etc.).

### 4.2 L'événement `PageEvent`

Un événement est défini par : le nom de la page, l'utilisateur, la date de visite et la durée de visite.

```java
package ma.enset.events;

import java.util.Date;

public record PageEvent(String name, String user, Date date, long duration) {
}
```

### 4.3 Service Producer : Rest Controller

Le contrôleur publie un `PageEvent` aléatoire dans le topic passé en paramètre, grâce à `StreamBridge`.

```java
@RestController
public class PageEventController {

    @Autowired
    private StreamBridge streamBridge;

    @GetMapping("/publish")
    public PageEvent publish(@RequestParam String name, @RequestParam String topic) {
        PageEvent event = new PageEvent(
                name,
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                10 + new Random().nextInt(10000));
        streamBridge.send(topic, event);
        return event;
    }
}
```

Test dans le navigateur : `http://localhost:8080/publish?name=P1&topic=R2`

![img_4.png](img_4.png)

Le message publié est reçu par le `kafka-console-consumer` abonné au topic `R2`.

### 4.4 Service Consumer

Un consumer est un `Bean` de type `Consumer<PageEvent>`.

```java
@Component
public class PageEventHandler {

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (input) -> {
            System.out.println("*********************");
            System.out.println(input.toString());
            System.out.println("*********************");
        };
    }
}
```

Configuration dans `application.properties` :

```properties
spring.cloud.function.definition=pageEventConsumer
spring.cloud.stream.bindings.pageEventConsumer-in-0.destination=R2
```

![img_5.png](img_5.png)
![img_6.png](img_6.png)

Dès qu'un message arrive dans le topic `R2`, il est consommé et affiché dans la console de l'application.

### 4.5 Service Supplier

Un supplier est un `Bean` de type `Supplier<PageEvent>` : il produit automatiquement un message à intervalle régulier (1 seconde par défaut). Cela permet de **simuler un flux** de données (par exemple un capteur).

```java
@Bean
public Supplier<PageEvent> pageEventSupplier() {
    return () -> new PageEvent(
            Math.random() > 0.5 ? "P1" : "P2",
            Math.random() > 0.5 ? "U1" : "U2",
            new Date(),
            10 + new Random().nextInt(10000));
}
```

Configuration :

```properties
spring.cloud.function.definition=pageEventConsumer;pageEventSupplier
spring.cloud.stream.bindings.pageEventSupplier-out-0.destination=R3
# Un message toutes les 200 ms
spring.cloud.stream.poller.fixed-delay=200
```

![img_7.png](img_7.png)

### 4.6 Service Kafka Streams : Real Time Stream Processing

Un traitement Kafka Streams est un `Bean` de type `Function<KStream<K,V>, KStream<K2,V2>>` : il lit un flux depuis un topic, effectue des calculs en temps réel et publie le résultat dans un autre topic.

- **Entrée** : topic `R3` (produit par le supplier)
- **Sortie** : topic `R4`

```java
@Bean
public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
    return (input) -> input
            .filter((k, v) -> v.duration() > 100)
            .map((k, v) -> new KeyValue<>(v.name(), v.duration()))
            .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))
            .count()
            .toStream()
            .map((k, v) -> new KeyValue<>(k, v));
}
```

Configuration :

```properties
spring.cloud.function.definition=pageEventConsumer;pageEventSupplier;kStreamFunction
spring.cloud.stream.bindings.kStreamFunction-in-0.destination=R3
spring.cloud.stream.bindings.kStreamFunction-out-0.destination=R4
```

**Contenu du topic `R3`** (flux produit par le supplier) :

![img_8.png](img_8.png)

**Contenu du topic `R4`** (résultat du traitement Kafka Streams) :

![img_9.png](img_9.png)

Pour afficher la **clé** et la **valeur** (de type `Long`) du topic `R4`, il faut préciser les deserializers dans le consumer console :

```bash
docker exec --interactive --tty broker kafka-console-consumer \
  --bootstrap-server broker:9092 \
  --topic R4 \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

![img_10.png](img_10.png)

### 4.7 Agrégation avec `groupByKey` / `groupBy`

L'opérateur `groupBy` regroupe les messages par clé (le nom de la page), puis `count()` compte le nombre de visites. Le résultat est un `KTable`, qu'il faut reconvertir en `KStream` avec `.toStream()`.

> Par défaut, Kafka Streams attend 30 secondes avant de publier les premiers résultats. Cet intervalle se règle avec la propriété suivante :

```properties
spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000
```

![img_11.png](img_11.png)

Le compteur est **cumulatif** depuis le démarrage du flux.

### 4.8 Fenêtrage avec `windowedBy`

Pour obtenir le nombre de visites uniquement sur les **5 dernières secondes**, on utilise un opérateur de fenêtrage. Le résultat est stocké dans une vue matérialisée nommée `count-store`.

```java
@Bean
public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
    return (input) -> input
            .filter((k, v) -> v.duration() > 100)
            .map((k, v) -> new KeyValue<>(v.name(), 0L))
            .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))
            .windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
            .count(Materialized.as("count-store"))
            .toStream()
            .map((k, v) -> new KeyValue<>(k.key(), v));
}
```

> **Point d'attention :** la durée de la fenêtre est exprimée en **millisecondes**. Écrire `5000` correspond bien à 5 secondes. Une confusion avec des secondes (fenêtre de 5000 secondes) empêche l'obtention de résultats avant très longtemps.

![img_12.png](img_12.png)

### 4.9 Exposition des résultats : endpoint `/analytics`

Le contrôleur interroge le store `count-store` via `InteractiveQueryService` et pousse les résultats vers le frontend chaque seconde en **Server-Sent Events**.

```java
@Autowired
private InteractiveQueryService interactiveQueryService;

@GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<Map<String, Long>> analytics() {
    return Flux.interval(Duration.ofSeconds(1))
            .map(sequence -> {
                Map<String, Long> stringLongMap = new HashMap<>();
                ReadOnlyWindowStore<String, Long> windowStore =
                        interactiveQueryService.getQueryableStore(
                                "count-store", QueryableStoreTypes.windowStore());
                Instant now = Instant.now();
                Instant from = now.minusMillis(5000);
                KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now);
                while (fetchAll.hasNext()) {
                    KeyValue<Windowed<String>, Long> next = fetchAll.next();
                    stringLongMap.put(next.key.key(), next.value);
                }
                return stringLongMap;
            });
}
```

Test dans le navigateur : `http://localhost:8080/analytics`

![img_13.png](img_13.png)

> Au démarrage, le flux est vide le temps que Kafka Streams calcule ses premiers résultats.

### 4.10 Application web de visualisation

Page statique `src/main/resources/static/index.html` (HTML + JavaScript) utilisant la bibliothèque **Smoothie.js** pour tracer des séries temporelles :

- Un `EventSource` s'abonne à `/analytics`.
- À chaque message, on lit `P1` et `P2`, puis on ajoute les points aux deux courbes.
- Deux `TimeSeries` sont affichées dans un `SmoothieChart` rattaché à un `canvas`.

```html
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>Real Time Analytics</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/smoothie/1.36.1/smoothie.min.js"></script>
</head>
<body>
<canvas id="chart2" width="600" height="400"></canvas>
<script>
    var index = -1;
    var pages = ["P1", "P2"];
    var colors = [
        {sroke: 'rgba(0, 255, 0, 1)', fill: 'rgba(0, 255, 0, 0.2)'},
        {sroke: 'rgba(255, 0, 0, 1)', fill: 'rgba(255, 0, 0, 0.2)'}
    ];
    var courbe = [];
    var smoothieChart = new SmoothieChart({tooltip: true});
    smoothieChart.streamTo(document.getElementById("chart2"), 500);

    pages.forEach(function (v) {
        courbe[v] = new TimeSeries();
        index++;
        smoothieChart.addTimeSeries(courbe[v], {
            strokeStyle: colors[index].sroke,
            fillStyle: colors[index].fill,
            lineWidth: 2
        });
    });

    var stockEventSource = new EventSource("/analytics");
    stockEventSource.addEventListener("message", function (event) {
        pages.forEach(function (v) {
            val = JSON.parse(event.data)[v];
            courbe[v].append(new Date().getTime(), val);
        });
    });
</script>
</body>
</html>
```

Accès : `http://localhost:8080/index.html`

**Résultat final :**

![img_14.png](img_14.png)

La courbe verte correspond à la page **P1** et la courbe rouge à la page **P2**. Elles montrent, en temps réel, le nombre de visites de chaque page sur les 5 dernières secondes.

---

## 5. Conclusion

Ce TP a permis de :

- démarrer un broker Kafka avec Zookeeper, en local puis avec Docker Compose ;
- tester la publication et la consommation de messages en ligne de commande ;
- développer avec Spring Cloud Stream un **producer** (`StreamBridge`), un **consumer** (`Consumer`) et un **supplier** (`Supplier`) ;
- réaliser du **Real Time Stream Processing** avec Kafka Streams : filtrage, `map`, `groupBy`, `count` et fenêtrage temporel ;
- interroger un state store avec `InteractiveQueryService` et diffuser les résultats vers une application web via Server-Sent Events.

Le pipeline complet est le suivant :

```
Supplier ──► Topic R3 ──► Kafka Streams ──► Topic R4
                              │
                         count-store ──► /analytics (SSE) ──► index.html (Smoothie.js)
```