Service that works like kafka connect with:
1. Mongo as a source or as a sink
2. Kafka as a source or as a sink


It distributes the load using Orleans. (Basically my playground for orleans)

Batching and parallelization is done using Dataflow.
Configuration is dynamic and depends on the type of source-sink or sink-source combo:
1. Mongo to mongo
2. Kafka to kafka
3. Mongo to kafka
4. Kafka to mongo

Examples of deployments can be found in deployment folder

Supports multi-cluster deployments using Konnect__Instance__Name env variable.
Each cluster can have multiple pods in kubernetes or standalone.