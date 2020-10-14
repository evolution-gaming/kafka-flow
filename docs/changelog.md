---
id: changelog
title: Changelog
sidebar_label: Changelog
---

# 0.2.x

## New features

- `PersistenceModule` trait to pass around persistence for all of keys, journals
and snapshots together and minimize the boilerplate. `CassandraPersistence` class
is a first implementation.
- `PartitionFlow`, `CassandraKeys`, `CassandraJournals` and `CassandraSnapshots`
do not require `MeasureDuration` anymore as it was not used anyway.

## Breaking changes

- `CassandraKeys.withSchema` requires `MonadThrowable` instead of `Fail` to
minimize custom DSL.
- `KafkaModule` renamed to `ConsumerModule` to reflect the purpose.
- `PartitionFlowOf.eagerRecoveryKafkaPersistence` in `kafka-flow-persistence-kafka`
module accepts `Tick` and `Fold` instead of `KeyFlowOf`.