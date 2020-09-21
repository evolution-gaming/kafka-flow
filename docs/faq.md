---
id: faq
title: FAQ
sidebar_label: FAQ
---

# Why the library eats a bit of CPU despite lack of incoming messages?

Kafka Flow is validating if any of the timers need to be triggered from time to time
during Kafka poll. If there are a lot of timers registered, the CPU usage could be
noticable even if there are no incoming messages.

If CPU usage is too high, one might opt for configuring `PartitionFlow` to do such
a triggering less often. See the `PartitionFlow` documentation in overview section
for more details.