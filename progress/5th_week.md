# Week5

2024-11-17

- Weekly Progress
    - master, worker cluster 기본 설정 (java 11, scala 2.13.15, sbt 1.8.3)
    - ScalaPB 코드를 서버에서 실행:
        - master→worker 성공
        - Worker와 Worker간의 communication 성공
        - worker → master x
    - proto 파일에 message 정의
    - gRPC 파일 전송 unit test 작성(로컬)
        - [332project/documents/gRPC File transfer.md at main · komseok0109/332project](https://github.com/komseok0109/332project/blob/main/documents/gRPC%20File%20transfer.md)
        - [shinjw4929/cs332_project_unit_test at gRPC_file_test](https://github.com/shinjw4929/cs332_project_unit_test/tree/gRPC_file_test)
    - ScalaPB 예시 코드를 이용하여 master와 worker 기본 구조 구현
    - input 파일 받아오기 관련 unit test 작성(로컬)
        - [332project/documents/gensort_getinput.md at main · komseok0109/332project](https://github.com/komseok0109/332project/blob/main/documents/gensort_getinput.md)
    - Sort, Partition 구현, 관련 unit test 작성(로컬)
        - [332project/documents/input, sort, partition.md at main · komseok0109/332project](https://github.com/komseok0109/332project/blob/main/documents/input%2C%20sort%2C%20partition.md)
        - [shinjw4929/cs332_project_unit_test at getInput_sort_partition](https://github.com/shinjw4929/cs332_project_unit_test/tree/getInput_sort_partition)
    - merge 기본 코드 구현
    - Design 문서 수정 (milestone, library, environment, flowchart, message정의 추가)
- Weekly Next Progress
    - sampling, merging 보완
    - master & worker 구현
- 개인 목표
    - 고민석:  master & worker
    - 하동은:  sampling, merging
    - 신재욱:  master & worker