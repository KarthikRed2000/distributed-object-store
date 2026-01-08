# Distributed Object Store

A high-performance, fault-tolerant distributed object storage system built from scratch in **Java 21**.

This system implements core distributed systems concepts including **Raft Consensus** for metadata coordination, **Consistent Hashing** for data distribution, and **Netty** for asynchronous non-blocking I/O.

## 🏗 Architecture

The system is organized into a multi-module Gradle project:

| Module              | Role                                                                                     | Key Technologies                                      |
| :------------------ | :--------------------------------------------------------------------------------------- | :---------------------------------------------------- |
| **`common`**        | Shared data models, serialization protocols, and utility logic.                          | Protobuf / Java Serialization                         |
| **`metadata-node`** | The "Brain". Manages cluster state, file maps, and node health using **Raft Consensus**. | **Raft Algorithm** (Leader Election, Log Replication) |
| **`storage-node`**  | The "Muscle". Responsible for physical disk I/O and serving chunks of data.              | **Netty**, Java NIO                                   |
| **`client-sdk`**    | The user-facing library to upload/download files. Hides complexity from the application. | Netty Client                                          |

## 🚀 Tech Stack

* **Language:** Java 21 (Virtual Threads ready)
* **Build System:** Gradle (Kotlin DSL)
* **Networking:** Netty 4.1 (Event-driven, asynchronous)
* **Consensus:** Custom Raft Implementation
* **Storage:** Local Disk I/O with Atomic Writes

---

## 🛠 Getting Started

### Prerequisites

* Java 21 or higher
* Gradle 8.0+ (Included via wrapper)

### Installation

Clone the repository and build the project:

```bash
git clone https://github.com/your-username/distributed-object-store.git
cd distributed-object-store
./gradlew clean build
```

### 🏃‍♂️ Running the System

#### 1. The Metadata Cluster (Raft)

We have a built-in `ClusterLauncher` that spawns a 3-node Raft cluster locally for testing.

**Via IntelliJ:**

Navigate to `metadata-node/src/main/java/com/distributed/store/metadata/ClusterLauncher.java` and run the main method. Watch the console for Leader Election logs.

**Via Terminal:**

```bash
./gradlew :metadata-node:run --args="ClusterLauncher"
```

#### 2. The Storage Node

To run a single storage node (which listens on port 8080 by default):

```bash
./gradlew :storage-node:run
```

## 📂 Project Structure

```
distributed-object-store/
├── common/             # Shared classes (RaftMessage, Chunk, NetworkMessage)
├── metadata-node/      # Raft logic (RaftNode, RaftTransport, Server)
├── storage-node/       # DiskStorage engine and Netty handlers
├── client-sdk/         # Client implementation
├── build.gradle.kts    # Root build configuration
└── settings.gradle.kts # Module definitions
```

## ✅ Features Implemented

* **Netty Transport Layer:** Custom efficient TCP messaging system using EventLoopGroups.
* **Raft Leader Election:**

    * Nodes start as Followers.
    * Timeouts trigger Candidacy.
    * Votes are requested and granted.
    * Leader is elected and maintains authority via Heartbeats.
* **Robust Networking:** Uses connection caching to prevent resource exhaustion (FD leaks).
* **Storage Engine:**

    * Atomic writes (.tmp -> rename) to prevent corruption.
    * Basic checksum validation.
* **Log Replication:** Replicating the "File Map" state across Raft nodes.
* **Consistent Hashing:** Logic to map files to specific storage nodes.
* **Client Integration:** Connecting the SDK to the Metadata Leader.

## 🚧 Roadmap / In Progress

* [ ] Erasure Coding: Implementing Reed-Solomon to split files into chunks.

## 🤝 Contributing

This is an educational project designed to explore the internals of Distributed Systems. Pull requests are welcome for optimizations or new features.

## 📄 License

MIT License.
