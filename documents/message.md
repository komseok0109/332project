# Message

## Initial & Sampling

1. **`registerWorker`**: Worker를 Master에 등록 
    
    Request (W → M)
    
    - 필드:
        - `String workerIP`: Worker의 IP 주소
    
    Reply (M → W)
    
    - 필드:
        - `int totalWorkerCount`: 전체 Worker의 수
        - `int workerID`: 해당 Worker의 ID
2. **`calculatePivots`**: Sampling Phase
    
    Request (W → M)
    
    - 필드:
        - `int workerID`: Worker의 ID
        - `repeated bytes sampleData`: 샘플 데이터
    
    Reply (M → W)
    
    - 필드:
        - `Map<int32, string> workerIPs`: Worker의 IP 맵핑
        - `repeated WorkerIDKeyRangeMapping keyRangeMapping`: Key Range와 Worker ID 맵핑
            - `int32 workerID`, `byte startKey`, `byte endKey`

---

## **Block Sorting & Partitioning**

1. **`partitionEndMsg`**: Partition 완료 메시지 *(Worker → Master)*

Request (W → M)

- 필드:
    - `int32 workerID`: Worker의 IDReply (M → W)

Reply (M → W)

- 필드:
    - `EmptyAckMsg`: 단순 확인 응답

---

## Shuffling

### **Shuffling 시작 전 Initial Setup**

1. **`startShuffling`: 모든 Worker의 Shuffling 시작 요청** *(Master → Worker)*
    
    **Request (M → W)**
    
    - **필드**:
        - `int32 receiver`: 수신 Worker ID
    
    **Reply (W → M)**
    
    - **필드**:
        - 없음 (`EmptyAckMsg`)

### **Shuffling 중**

1. **`sendDataToWorker`**: **Shuffling 중 데이터 전송 과정**
    
    **Request (Source Worker → Destination Worker)**
    
    - **필드**:
        - `repeated bytes data`: 전송할 데이터
        - `string filename`: 데이터 송신 Worker ID
    
    **Reply (Destination Worker → Source Worker)**
    
    - **필드**:
        - 없음 (EmptyAckMsg)
2. **`shuffleAck`**: **Shuffling 완료 메시지** *(Worker → Master)*
    
    **Request (Worker -> Worker)**
    
    - **필드**:
        - `int32 source`: 데이터 송신 Worker ID
    
    **Reply (Worker -> Worker)**
    
    - **필드**:
        - 없음 (EmptyAckMsg)

---

## Merge Sort

**`mergeEndMsg`**: Merge 완료 메시지 *(Worker → Master)*

Request (W → M)

- 필드:
    - `int32 workerID`: 작업 완료한 Worker IDReply (M → W)
- 필드:
    - 없음 (EmptyAckMsg)