# Change log
## 0.4.29 - 2021-09-06
### Notable changes
  - Scala 2.12.11 is updated to 2.12.14
  - kafka-flow's internal `RebalanceListener` helper now returns new freemonad-based `RebalanceListener1` since the old skafka's `RebalanceListener` is now deprecated
  - kafka-flow's simplified `Consumer` now also uses `RebalanceListener1`
### Updated dependencies
  - kafka-journal updated from 0.0.152 to 0.0.161. Note that these versions are binary incompatible.
  - skafka updated from 11.0.0 to 11.5.0. This also transitively updates kafka-clients from 2.5.0 to 2.7.1
  - kafka-launcher updated from 0.0.10 to 0.0.11
#### Plugin updates
  - sbt-scoverage from 1.6.1 to 1.8.2
  - sbt-coveralls from 1.2.7 to 1.3.1