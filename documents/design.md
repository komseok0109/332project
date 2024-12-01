# Distributed Sorting Design Report

## Programming Environment
JDK: 11
SBT: 1.8.3
scala: 2.13.15

### Milestones

- **Milestone #0: Define Commit Convention and Coding Style**
  - Establish commit message conventions and code formatting guidelines.

- **Milestone #1: Generate Input Data Using Gensort**
  - Use the `gensort` tool to create input data for testing.

- **Milestone #2: Familiarize with gRPC by Writing Example Code**
  - Learn to configure `scalapb` for Scala-based gRPC.
  - Write basic example code to understand gRPC operations.

- **Milestone #3: Define gRPC Messages and Fields**
  - Break down the distributed sorting process into four phases.
  - Define the messages and fields required for each phase in gRPC.

- **Milestone #4: Select Libraries**
  - Research and choose libraries for file I/O, synchronization, and asynchronous operations.

- **Milestone #5: Implement Master and Worker for Command Execution**
  - Implement server start and stop functionality.
  - Parse and apply command-line arguments for distributed sorting execution.

- **Milestone #6: Implement Phase 1 - Sampling**
  - Workers sample data randomly and send samples to the master.
  - The master collects samples and determines key ranges.

- **Milestone #7: Implement Phase 2 - Sorting and Partitioning**
  - Sort each input block and save as a temporary file.
  - Partition each block and group keys by range into dedicated blocks.

- **Milestone #8: Implement Phase 3 - Shuffling**
  - Design and implement a method for data shuffling between workers.

- **Milestone #9: Implement Phase 4 - Merging**
  - Merge sorted blocks within each worker.
  - Select an efficient merging algorithm or library, and design parallelized merging.

- **Milestone #10: Implement Testing and Validation**
  - Design unit and integration tests for each phase to verify correctness and performance.

## Libraries 
- File I/O: `java.io`, `java.nio`   
- Synchronization: `CountDownLatch`
- Asynchronization: `Future`
- For data: `ByteString`, `scala.collection.concurrent`
- network: `io.gRPC`, `protobuf` (ScalaPB)
- Logging: `LazyLogging`

## Overall Flow Chart
![image.png](images/flowchart.png)

## Message
<table>
  <tr>
    <th>service</th>
    <th colspan="2">Master <-> Worker</th>
  </tr>
  <tr>
    <td rowspan="5">rpc</td>
    <td><code>registerWorker</td>
    <td>worker-to-master server connection</td>
  </tr>
  <tr>
    <td><code>calculatePivots</td>
    <td>Determine the pivot based on sampled data and distribute it to the workers.</td>
  </tr>
  <tr>
    <td><code>partitionEndMsg</td>
    <td>signaling the end of a worker’s partitioning</td>
  </tr>
  <tr>
    <td><code>shufflingEndMsg</td>
    <td>worker notified shuffling done</td>
  </tr>
  <tr>
    <td><code>mergeEndMsg</td>
    <td>confirming completion of merging</td>
  </tr>
</table>

<table>
  <tr>
    <th rowspan="1">rpc</th>
    <th colspan="2">registerWorker : register each worker to master</th>
  </tr>
  <tr>
    <th rowspan="4">message</th>
    <td rowspan="1">RegisterWorkerRequest<br>(worker -> master)</td>
    <td><code>string workerIP</code> : [worker's IP address]</td>
  </tr>
  <tr>
    <td rowspan="2">RegisterWorkerReply<br>(master -> worker)</td>
    <td><code>int32 totalWorkerCount</code> : [total number of workers]</td>
  </tr>
    <tr>
    <td><code>int32 workerID</code> : [worker's ID number]</td>
  </tr>
</table>

<table>
  <tr>
    <th rowspan="1">rpc</th>
    <th colspan="2">calculatePivots : Determine the pivot based on sampled data and distribute it to the workers.</th>
  </tr>
  <tr>
    <th rowspan="4">message</th>
    <td>CalculatePivotRequest<br>(worker -> master)</td>
    <td>
      <code>int32 workerID</code> : [worker's ID number, to say which worker's sampling ended]<br>
      <code>repeated bytes sampleData</code> : [worker's sampled data]
    </td>
  </tr>
  <tr>
    <td>CalculatePivotReply<br>(master -> worker)</td>
    <td>
      <code>map&lt;int32, string&gt; workerIPs</code> : [map { workerID -> workerIP }] <br>
      <code>repeated WorkerIDKeyRangeMapping keyRangeMapping</code> : list of WorkerIDKeyRangeMapping defined as <code>message {
        int32 workerID = 1;
        bytes startKey = 2;
    bytes endKey = 3;
      }</code>
    </td>
  </tr>
</table>

<table>
  <tr>
    <th rowspan="1">rpc</th>
    <th colspan="2">partitionCompleteMsg : signals to the master that data partitioning by each worker is complete.
    </th>
  </tr>
  <tr>
    <th rowspan="4">message</th>
    <td>PhaseCompleteNotification
    <br>(worker -> master)</td>
    <td>
      <code>int32 workerID</code>
    </td>
  </tr>
  <tr>
    <td>EmptyAckMsg<br>(master -> worker)</td>
    <td>
      empty 
    </td>
  </tr>
</table>


<table>
  <tr>
    <th rowspan="1">rpc</th>
    <th colspan="2">shufflingEndMsg : worker shuffling done
    </th>
  </tr>
  <tr>
    <th rowspan="4">messages</th>
    <td>PhaseCompleteNotification
    <br>(worker -> master)</td>
    <td>
      <code>int32 workerID</code>
    </td>
  </tr>
  <tr>
    <td>EmptyAckMsg<br>(master -> worker)</td>
    <td>
      empty 
    </td>
  </tr>
</table>

<table>
  <tr>
    <th rowspan="1">rpc</th>
    <th colspan="2">mergeEndMsg : signals to the master that merging by each worker is complete
    </th>
  </tr>
  <tr>
    <th rowspan="4">messages</th>
    <td>PhaseCompleteNotification
    <br>(worker -> master)</td>
    <td>
      <code>int32 workerID</code>
    </td>
  </tr>
  <tr>
    <td>EmptyAckMsg<br>(master -> worker)</td>
    <td>
      empty 
    </td>
  </tr>
</table>

<table>
  <tr>
    <th rowspan="1">service</th>
    <th colspan="2">Worker <-> Worker
    </th>
  </tr>
  <tr>
    <th rowspan="4">rpc</th>
    <td>sendDataToWorker</td>
    <td>
      send data from one worker to others
    </td>
  </tr>
  <tr>
    <td>shuffleAck</td>
    <td>
      Notify receiver that all data has been sent.
    </td>
  </tr>
</table>

<table>
  <tr>
    <th rowspan="1">rpc</th>
    <th colspan="2">sendDataToWorker
    </th>
  </tr>
  <tr>
    <th rowspan="4">messages</th>
    <td>sendDataRequest
    </td>
    <td>
      <code>repeated byte datas</code> : [List of data to be sent]
      <br><code>string filename</code> : [Name of the file source of data]
    </td>
  </tr>
  <tr>
    <td>EmptyAckMsg</td>
    <td>
      empty 
    </td>
  </tr>
</table>

<table>
  <tr>
    <th rowspan="1">rpc</th>
    <th colspan="2">shuffleAck: Notify receiver that all data has been sent.
    </th>
  </tr>
  <tr>
    <th rowspan="4">messages</th>
    <td>shuffleAckRequest
    </td>
    <td>
    <code>int32 source</code>
    </td>
  </tr>
  <tr>
    <td>EmptyAckMsg</td>
    <td>
      empty 
    </td>
  </tr>
</table>

## Sampling
In the sampling phase, each worker extracts 1MB of data from the input directory and sends it to the master. The master collects the 1MB samples received from all workers, sorts them, and divides the range into segments based on the number of workers to calculate pivots. Using the calculated pivots, the master assigns key ranges to each worker.

## Sorting & Partitioning
After receiving key ranges, workers read files from their input directories, sort the data, and perform partitioning. During this process, they read 100MB chunks at a time into memory, sort the data, and partition it based on the assigned key ranges, saving the partitions as separate files.

In other words, if there are N workers, each 100MB chunk is divided into N files and stored accordingly.

## Shuffling
Each partition is sent to the worker it is designated for. Multiple threads are created to send partitions to multiple workers simultaneously. Additionally, each worker runs a server to receive and store partitions sent to it.

## Merging
During the merging process, the shuffled partitions are repartitioned into 4 consecutive ranges. Each range is assigned its respective partitions, which are then merged. This process results in a total of 4 output files, each corresponding to one of the defined ranges.











