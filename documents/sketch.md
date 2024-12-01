# Distributed Sorting Design Sketch

## Requirement
Distributed/Parallel sorting key/value records stored on multiple disks on multiple machines

## Implementation Outline

### 마스터 노드 기능
- 워커의 IP와 포트 설정하고, 통신을 위한 gRPC 서버 시작
- 각 워커로부터 샘플링된 데이터를 수신하여 통합하고, 전체 데이터를 정렬
- 정렬된 데이터를 기반으로 pivot point를 계산하여 워커에 전달
- 각 워커에 대해 계산된 pivot point 사용하기 위해 key range 할당
- 각 워커마다 작업이 완료된 후에 다음 phase로 넘어가는 신호를 보낸다

### 워커 노드 기능
- 마스터 노드와의 연결을 설정하고, 입력받은 입력 및 출력 디렉토리 설정
- 지정된 입력 디렉토리에서 데이터 블록을 읽어오기
- 각 데이터 블럭 정렬 후, 키 범위에 따라 파티셔닝
- 파티셔닝 후, 다른 워커 노드들과 셔플링을 통해 피벗에 따라 데이터를 교환
- 셔플링 후, 각 블록들을 병합

### gRPC
- 마스터와 워커 간의 통신을 위한 gRPC 서비스 및 메서드 정의 필요
- 데이터 전송, pivot 정보 요청 등등
  
## Sketch
과제 구현은 크게 4가지 phase로 나뉘어진다.

1. Initial setup & Sampling
    
    - Master가 Worker의 data: worker 수, Worker IP 주소 등을 받아옴.
    
    - Worker가 input&output directory, Master IP주소
    
    - Worker가 데이터를 받아온 후, sample data를 Master로 전달
    
    - Master는 sample data를 sort하고, Pivot을 계산함
    
    - Pivot에 따라 각 Worker가 가지는 key range를 정한다.
    
    - key range - Worker mapping data와, 각 Worker의 IP 주소를 모든 Worker에 전달하고, Sorting & Partitioning을 시작하는 신호를 전달한다. 여기서 key range의 순서에 따라 Worker의 순서가 부여된다.
    
2. Sorting & Partitioning
    
    - Worker가 block 단위로 sort 진행하고, 각 block을 key range에 따라 partitioning을 진행한다. 
    각 block은 여러 개의 part로 구성되고, part 안의 모든 data는 같은 Worker로 전달되어야 한다.
    
    - 모든 block에 대해 partition을 완료했다면, part 순서대로 queue에 삽입한다. Worker에 순서를 부여하여, 해당 Worker에 전달되어야 하는 part를 순서대로 정렬한다.
    
    - 예를 들어, Worker에 W1 W2 W3 과 같이 순서를 부여했다고 가정할 때, 모든 Worker 안의 block은 P1, P2, P3 과 같이 나누어진다. 이후 P1 끼리, P2 끼리, P3 끼리 모아 순서대로 queue에 삽입한다.
    
    - 구현 방식은 disk 전체를 Worker 개수 만큼 탐색하며, P_i 에 해당하는 part를 queue에 삽입하는 방식으로 한다. 전부 탐색한 이후 queue에는 (P1) - (P1) - … - (P1) - (P2) … 와 같은 방식으로 정렬될 것이다.
    
    - Worker가 Partitioning을 완료하면 Master에 신호를 보낸다. 모든 Worker가 작업을 완료하면 Master가 Shuffling을 시작한다.
    
3. Shuffling
    
    - Shuffling은 하나의 Worker를 선택하여 집중적으로 처리하는 방식을 선택하였다.  하나의 Worker W_i를 정하여, P_i 를 모두 W_i에 옮긴 후 다음 Worker W_i+1 에 대해 진행하는 방식으로 구현한다.
    
    - 예를 들어 W1 W2 W3 이 있다고 하면, 먼저 W1에 들어가야 할 P1 을 모든 Worker에서 받아온 후, W1 탐색을 종료한다. 이후 W2, W3 을 탐색하게 되는데, 이때 W1 안의 data는 전부 P1 이 존재하므로 W을 탐색하지 않아도 된다.
    
    - 현재 탐색할 Worker는 Master에서 정해진다. 즉 Master 에서 Worker 순서대로 loop을 돌아가며 현재 탐색할 Worker를 정해준다.
    
    - Worker 끼리 data를 주고받는 과정은 다음과 같이 이루어진다. W1을 집중적으로 탐색한다고 가정하자.
    
    - 먼저 W1에서 탐색을 할 때 (자신이 자신에게 data를 전송해야 할 때) : queue의 가장 앞 부분인 P1을 pop 한 후, queue에 다시 삽입한다.
    P1 - P2 - P3 형태의 queue를  P2 - P3 - P1 형식으로 바꾸는 것이다.
    
    - 이후 W2에서 탐색을 할 때 (다른 Worker가 data를 전송할 때) : queue의 가장 앞 부분 P1을 W1으로 넘겨준다. 이 때, 넘겨주는 Worker의 정보( W2 )와 P1 data를 넘겨준다.
    
    - W1은 W2에서 data를 받았다는 신호를 받을 때 (data를 받은 Worker에게 data를 돌려줄 때): 해당 Worker (W2)에게 data를 넘겨준다. 이 때 W2로 넘겨주는 data의 크기는 받은 data의 크기와 동일하게 하며, queue의 앞 부분 data 부터 넘겨준다.
    
    - W1 에게 data를 넘겨주는 과정을 모든 Worker에 대해 진행하게 되면 모든 P1은 W1에 저장되므로 W1에 대한 탐색을 종료한다.
    이후 W2에게 집중적으로 data를 넘겨주는 과정에서는 W1을 탐색하지 않아도 된다. W1 안에는 P1 내용밖에 없기 때문이다.
    
    - 각 Worker에 대한 shuffling이 끝나면 해당 Worker는 Merge sorting으로 바로 넘어간다. 즉 W2를 집중적으로 탐색하는 도중 W1은 Merge Sort를 진행하는 것이다.
    
4. Merge sorting
    
   -  4개의 Core 를 사용한다는 장점을 살리기 위해 data를 다시 partitioning 한다. 4개의 key range로 나누어 순서대로 정렬한다.
    
   - 이후 각 range에 대해 merge sort를 진행하면 disk 내의 모든 data가 정렬된다.
    

Master과 Worker에게 필요한 기능은 다음과 같다.

- Master
    - Argument Parsing : Initial setup 과정, Worker에서 받은 신호를 해석
    - Start Message : Worker에게 다음 Phase 로 넘어가라는 Message를 보내야 한다.
    - Worker에서 받은 Sample을 sort 하여 pivot을 계산하는 역할
    - Worker의 순서를 부여하고, key range와 Worker mapping을 만드는 역할
    - key range - Worker mapping과 각 worker 주소를 전달하는 역할
    - Shuffling 도중 loop 을 구성하여 Worker를 탐색하는 역할
    
- Worker
    - Argument Parsing : Initial setup 과정, Master 및 각 Worker에게 받은 신호를 해석
    - End Message : Master에게 Phase가 끝남을 알리는 Message를 보냄
    - Master에게 받은 key range 에 따라 Partitioning 하는 역할
    - disk를 탐색하며 queue에 part를 순서대로 삽입하는 역할
    - 자신이 자신에게 data를 보낼 때 queue 앞의 data를 pop 하고 다시 push 하는 역할
    - 다른 Worker에서 data를 받는 역할
    - 다른 Worker에게 queue 맨 앞의 data를 보내는 역할
    - disk 내의 모든 part에서 sample을 뽑아 sort 한 뒤, 4개의 key range로 나누는 역할
    - 나누어진 4개의 key range에 따라 disk 내에서 shuffling하는 역할
    - 각 key range에 대해 merge sort 하는 역할 : concurrent merge sort

