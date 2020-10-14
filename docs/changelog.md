---
id: chagelog
title: Changelog
sidebar_label: Changelog
---

# 0.2.0

## New features

- `PersistenceModule` trait to pass around persistence for all of keys, journals
and snapshots together and minimize the boilerplate. `CassandraPersistence` class
is a first implementation.
- `CassandraKeys`, `CassandraJournals` and `CassandraSnapshots` do not require
`MeasureDuration` anymore as it was not used anyway.

## Breaking changes

- `CassandraKeys.withSchema` requires `MonadThrowable` instead of `Fail` to minimize custom DSL.
- `KafkaModule` renamed to `ConsumerModule` to reflect the purpose.