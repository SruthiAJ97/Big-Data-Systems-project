# Distributed Dependency Miner (Akka)

This project implements a distributed Inclusion Dependency (IND) discovery system using Akka Typed (Java). It processes large CSV datasets and identifies column relationships across files using a Master–Worker architecture.

---

## 🚀 Features

- Distributed processing using Akka actors
- Handles large datasets via data partitioning
- Scalable multi-worker architecture
- Unary Inclusion Dependency (IND) discovery
- Distributed data storage using DataProvider actors

---

## ⚙️ How It Works

1. Headers are read and columns are registered
2. Workers register with DependencyMiner along with their DataProvider
3. Columns are assigned to workers:
   ownerIndex = abs(hash(columnRef)) % numWorkers
4. Data is read in batches and distributed to DataProviders
5. Workers perform IND checks
6. Results are collected and finalized

---

## 🛠️ Build & Run

### Build

```
mvn clean package
```

### Run

```
java -jar target/ddm-akka.jar master -w 1 -es false
```

## ⚡ Key Design Decisions

- No centralized storage in master
- One DataProvider per JVM
- Immutable message passing between actors
- Round-robin task distribution

## 📌 Future Improvements

- Support n-ary inclusion dependencies
- Optimize batching and communication
- Enable multi-node cluster execution
