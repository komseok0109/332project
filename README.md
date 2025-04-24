# Distributed Sorting System

A distributed sorting system developed as part of the **CSED332: Software Design Methods** course at POSTECH.  
This project demonstrates how to design a scalable and parallel sorting system using a master-worker architecture, written in Scala and built with SBT. The system distributes sorting workloads to multiple worker nodes, aggregates the results, and validates correctness using `valsort`.

---
## Table of Contents

- [Project Overview](#project-overview)
- [Installation](#installation)
- [Build Instructions](#build-instructions)
- [Running the System](#running-the-system)
- [Testing](#testing)
- [Team Members](#team-members)

---
## Project Overview

This system performs distributed sorting over large datasets across multiple machines.  
It features:
- A master process that assigns sorting tasks and collects sorted data.
- Worker processes that sort assigned data chunks locally.
- Inter-process communication over gRPC.
- Validation through `valsort` to ensure correct global sorting.
---
## Installation
### Requirements
- Java 11
- Scala 2.13
- SBT 1.8+

### Setup Instructions

```bash
# Download and extract necessary packages
wget https://download.java.net/openjdk/jdk11.0.0.2/ri/openjdk-11.0.0.2_linux-x64.tar.gz
wget https://downloads.lightbend.com/scala/2.13.15/scala-2.13.15.tgz
wget https://github.com/sbt/sbt/releases/download/v1.8.3/sbt-1.8.3.tgz

tar -xvf openjdk-11.0.0.2_linux-x64.tar.gz
tar -xvf scala-2.13.15.tgz
tar -xvf sbt-1.8.3.tgz
```
Update your `~./bashrc`:
```
export PATH=/home/yourname/jdk-11.0.0.2/bin:$PATH
export PATH=/home/yourname/scala-2.13.15/bin:$PATH
export PATH=/home/yourname/sbt/bin:$PATH
source ~/.bashrc
```
## Build Instructions
Clone and build on each node (master and workers):
```
git clone https://github.com/komseok0109/332project.git
cd 332project/DistributedSorting
sbt compile
```
## Running the System
1. Ensure all machines have input and oupt directories set up.
2. Start the master:
   ```
   sbt
  > runMain machine.Master [NumberOfWorkers]
   ```
3. Start each worker after the master server is up:
```
sbt
> runMain machine.Worker [MASTER_IP]:50051 -I [INPUT_PATHS] -O [OUTPUT_PATH]
```
- INPUT_PATHS: space-separated list of input directories.
- Ensure the output directory is empty before each run.
## Testing
Each worker produces output files named partition[w][p]. To validate output:
```bash
# On each worker
./valsort -o 1.sum output/partition[w]1
./valsort -o 2.sum output/partition[w]2
...
cat 1.sum 2.sum 3.sum 4.sum > all[w].sum
./valsort -s all[w].sum
```
Transfer all .sum files to the master and combine:
```bash
scp white@2.2.2.101:/home/white/all1.sum ~
scp white@2.2.2.102:/home/white/all2.sum ~
...
cat all1.sum all2.sum ... allN.sum > all.sum
./valsort -s all.sum
```
## Team Members
[고민석](https://github.com/komseok0109)
[신재욱](https://github.com/shinjw4929)
[하동은](https://github.com/Binny-B)
