# 332Project

Distributed Sorting Project

## Index
[1. Teammates](#teammates)
[2. Configuration](#configuration)
[3. Weekly Progress](#weekly-progress)
[4. Milestone](#milestone)


## Teammates

[고민석](https://github.com/komseok0109)
[신재욱](https://github.com/shinjw4929)
[하동은](https://github.com/Binny-B)

## Configuration

| **Software** | **Version** |
|--------------|-------------|
| JDK          | 11.0.0.2    |
| Scala SDK    | 2.13.15     |
| SBT          | 1.8.3       |

| **Category**            | **Library/Framework**                                   | **Description**                                                           |
|-------------------------|--------------------------------------------------------|---------------------------------------------------------------------------|
| **File I/O**            | `java.io`                                              | Standard Java library for basic file and stream handling.                |
|                         | `scala.io`                                             | Scala library for reading and writing files and streams.                 |
|                         | `java.nio`                                             | Advanced Java library for non-blocking I/O and file channels.            |
| **Network**             | `scalapb`                                              | Protocol Buffers (Protobuf) support for Scala.                           |
|                         | `io.grpc.grpc-netty`                                    | gRPC communication using the Netty transport.                            |
| **Concurrent Programming** | `CountDownLatch`                                     | Synchronization aid for threads waiting for a task to complete.          |
|                         | `AtomicInteger`                                        | Provides atomic operations on integers for thread safety.                |
|                         | `synchronized`                                         | Keyword for synchronizing critical sections in Scala and Java.           |
|                         | `Future` (`scala.concurrent`, `scala.util`)            | Asynchronous computation abstraction in Scala.                           |
| **Logging**             | `com.typesafe.scalalogging.LazyLogging` (SLF4J)         | Scala logging framework based on SLF4J for structured logging.           |
| **Additional**          | `Cats Effect`                                          | Library for functional programming and effect management.                |
|                         | `scala-parallel-collections`                           | Parallel collections library for Scala.                                  |

### Commit & Pull request convention
- `Feature`: Add new function
- `Fix`: Fix bug
- `Docs`: Modify Document
- `Chore`: Change Settings (build, project configs...)
- `Test`: Add/Fix Test suite
- `Refactor`: Refactor code


## Weekly progress

### week 1
- Define commit conventions
- Define coding style
- Schedule meeting time
- Define milestones

### week 2
- Study the project scope
- Draft a simple design
- Explore how to effectively use ChatGPT

### week 3
- Develop a detailed design 
- Gain a deeper understanding of gRPC

### week 4
- Write gRPC example code
- Define message protocols
- Generate input data

### week 5
- Set up virtual machines
- Begin implementation
- Conduct unit testing



## Milestone

- Milestone #0: ~~Define Commit Convention and Coding Style~~
- Milestone #1: ~~Generate Input Data Using Gensort~~
- Milestone #2: ~~Familiarize with gRPC by Writing Example Code~~
- Milestone #3: ~~Define gRPC Messages and Fields~~
- Milestone #4: ~~Select Libraries~~
- Milestone #5: ~~Implement Master and Worker(communication, command execution)~~
- Milestone #6: Implement Phase 1 - Sampling

- Milestone #7: Implement Phase 2 - Sorting and Partitioning

- Milestone #8: Implement Phase 3 - Shuffling

- Milestone #9: Implement Phase 4 - Merging
- Milestone #10: Implement Testing and Validation


