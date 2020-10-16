---
id: changelog
title: Changelog
sidebar_label: Changelog
---

# 0.3.x

## Breaking changes

- `PartitionFlowOf` lost `applicationId` and `groupId` parameters as these are
already specified in `KeyStateOf` constructor and it was impolite to requre them
twice.
- For sake of simplification of API `KeyStateOf` is no more polymorphic for
the key (using `String` and `KafkaKey`) and incoming records (always using
`ConsRecord` now).

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
- `Tick.unit` and `TickOption.unit` renamed to `Tick.id` and `TickUnit.id`.
- `PartitionFlowOf.eagerRecoveryKafkaPersistence` in `kafka-flow-persistence-kafka`
module accepts `Tick` and `Fold` instead of `KeyFlowOf`.