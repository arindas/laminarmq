<p align="center">
  <img src="https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/logo.png" alt="laminarmq">
</p>

<p align="center">
  <a href="https://github.com/arindas/laminarmq/actions/workflows/book.yml">
  <img src="https://github.com/arindas/laminarmq/actions/workflows/book.yml/badge.svg" alt=""/>
  </a>
</p>

<p align="center">
A scalable, distributed message queue powered by a segmented, partitioned, replicated and immutable log.
</p>

`laminarmq` presents an elementary commit-log abstraction (a series of records ordered by offsets), on top of which
several message queue semantics such as publish subscribe or even full blown protocols like MQTT could be implemented.
Users are free to read the messages with offsets in any order they need.

This book is meant to answer the "Why?" and "How?" questions behind `laminarmq` and it's internal design. The "What?"
questions are extensively covered in [API Documentation](https://docs.rs/laminarmq).

## License
`laminarmq` is licensed under the MIT License. See [License](./LICENSE) for more details.
