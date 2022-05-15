# Orion
<img src="/src/main/resources/orion-constellation.webp?raw=true" width="350" height="300">

Configurable data lineage solution for Apache Spark

# About Project

The project aims at providing pluggable solution for Apache spark projects to capture the lineage of the data assets being sourced and created in the application.
The project supports both Dataframe APIs as well as raw queries submitted using spark sql APIs.

# Event Handling

The captured lineage events can be forwarded to variety of supported backends.

The supported backends are:
- Console: It is primarily used for debugging purpose and it will log the captured events
- Neo4J: It will push the lineage events towards Neo4j and store the lineage graph as Neo4j graphs
- HTTP: It will push the events to an API server.
- Kafka: It will push the events to a Kafka topic.

# Configuration

Orion can be plugged into any Spark application using the below configuration which can be set at cluster level or individual Spark conf level:

```
spark.sql.queryExecutionListeners=com.mkv.ds.orion.core.listeners.OrionQueryInterceptor

```

- Console backend can be configured with spark configuration: _spark.orion.lineage.backend.handler=Console_
- Neo4J backend can be configured with spark configuration: _spark.orion.lineage.backend.handler=Neo4J_
- HTTP backend can be configured with spark configuration: _spark.orion.lineage.backend.handler=HTTP_
- Kafka backend can be configured with spark configuration: _spark.orion.lineage.backend.handler=KAFKA_

[Kafka & HTTP backend implementations to be added in a future release]

# Neo4J Configuration
To push event towards a Neo4J database below configurations have to be set:
```JAVA
spark.orion.lineage.backend.handler="NEO4J"
spark.orion.lineage.neo4j.backend.uri="bolt://localhost:7687"
spark.orion.lineage.neo4j.backend.username="neo4j"
spark.orion.lineage.neo4j.backend.password="password"
```

Running _OrionTestDriver_ class with Neo4j backend will capture the below lineage in Neo4j

<img src="/src/main/resources/lineage_graph.png?raw=true">
<img src="/src/main/resources/Node_Labels.png?raw=true">
