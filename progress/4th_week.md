# Week4

2024-11-07

- Weekly Plan
    - week1 : 전체적 계획 수립
    - week2 : gRPC, 프로젝트 이해
    - week3 : gRPC테스트 코드 작성, 디자인
    - week4 : 구체적인 디자인, 코드 작성
    - week5 : 코드 작성 완료, 테스트 케이스 작성
    - week6 : 디버깅, 중간 발표 준비
    - week7 : 디버깅
    - week8 : 최종 보고서, 발표 준비
- Milestone
    - Master와 Worker간의 communication 구현
    - Sampling 방법 설정 및 구현
    - sort어떻게 할 것인지 (2-way merge, k-way merge) + merge 방법 설정 및 구현
    - shuffling 구현
- Weekly Progress
    - gRPC에 대해 공부하고 scala 테스트 코드 작성 완료
        - https://github.com/komseok0109/332project/blob/main/documents/gRPC.md
        - [shinjw4929/cs332_project_gRPC at gRPC_test_1](https://github.com/shinjw4929/cs332_project_gRPC/tree/gRPC_test_1)
    - gRPC에 대해 공부한 후 메시지를 우선 구체적으로 정의해야겠다 생각이 들어서 어떤 메시지가 필요하고 메시지 안에 어떤 필드가 필요한지 정의
        - https://github.com/komseok0109/332project/blob/main/documents/Message_config.md
    - ChatGPT 활용하여 shuffling관련 디자인 변경 (병렬화)
        - https://github.com/komseok0109/332project/blob/main/documents/chatgpt.pdf
    - gensort 실행하여 input data 생성
    
- Weekly Next Progress
    - 다음 주에는 자주 모여서 코드 작성

- 개인 목표
    - 고민석:  코드 작성
    - 하동은:  코드 작성
    - 신재욱:  코드 작성