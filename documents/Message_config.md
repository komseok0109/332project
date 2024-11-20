# Message

1. Initial & Sampling
    - Worker를 Master에 등록 (Worker → Master)
        
        Request (W → M)
        
        - String workerIP
        
        Reply: shuffling에 이용하기 위해 각 worker들의 IP 주소 (M → W)
        
        - int totalWorkerNumber
        - Map { int (workerNumber) → String (worker_IP) }
    - Sampling Phase
        
        Request: Worker의 sample data를 Master에 전송 (W → M)
        
        - int workerNumber
        - String samplingData
        
        Reply: Master의 정보와 key range, Worker 순서 (M → W)

        - Map { Pair (Key range) → int (workerNumber) }
    
2. Block sorting & Partitioning
    - Partition 완료 Message (Worker → Master)
        
        Request (W→ M)
        
        - int workerNumber
        
        Reply (M → W)
        
        - bool isShuffleInitStarted
        
3. Shuffling
    
    —shuffling 시작 전 initial setup
    
    - 각 worker의 남은 용량 (Worker → Master)
        
        Request (W → M)
        
        - int freeVolumeOfWorker
        
    - 모든 worker의 shuffling을 start (Master → Worker)
        
        Request (M → W)
        
        - bool isShuffleStarted
        
    
    —shuffling 중, data 전송 과정
    
    - 각 worker들의 정보 요청 (Worker → Master)
        
        Request (W → M)
        
        - bool isWorkerInfoRequested
        
        Reply (M → W)
        
        - Map { int (workerNumber) →  int (freeVolumeOfWorker) }
        - Map { int (workerNumber) → bool (isWriteDenied) }
        
    - 어떤 worker에 data를 보낼 지 Master에게 정보 전달 (Worker → Master)
        
        Request (W → M)
        
        - int srcWorkerNumber
        - int dstWorkerNumber
        
    - Source - Destination 정보 교환 (Source Worker → Destination Worker)
        
        Request (Src → Dst)
        
        - String data
        
        Reply (Dst → Src)
        
        - bool isDataRecieved
        
    - Src-Dst data 전달 종료 선언 (Source Worker → Master)
        
        Request (Src → M)
        
        - bool isDataRecieved
        - int freeVolumeOfSrcWorker
    
    - Src-Dst data  종료 선언 (Destination Worker → Master)
        
        Request (Dst → M)
        
        - int freeVolumeOfDstWorker
    
    - Shuffling 완료 메세지 (Worker → Master)
        
        Request (W → M)
        
        - bool isShufflingEnded
        
    - Merge 시작 메세지 (Master → Worker)
        
        Request (Master → Worker)
        
        - bool isMergeStarted
4. Merge sort
    - merge완료 (Worker → Master)
        - int workerNumber
