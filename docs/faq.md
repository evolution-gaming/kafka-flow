---
id: faq
title: FAQ
sidebar_label: FAQ
---

# Why the library eats a bit of CPU despite lack of incoming messages?

Kafka Flow is validating if any of the timers need to be triggerd during Kafka poll.
If there are a lot of timers registered, the CPU usage could be more noticable than
just an empty poll. In future smarter logic might be implemented to make this process
more lightweight or configurable, so it happens more rarely.