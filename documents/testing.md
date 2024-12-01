# Distributed Sorting Testing

Dataset(/dataset/large) Each worker starts with 1000 input files (3.2 GB)

Master: sbt "runMain machine.Master 10"
Worker: sbt "runMain machine.Worker 2.2.2.254:50051 -I /home/dataset/large -O /home/white/output"

Worker ID 1:
![image.png](images/worker1.png) 

Worker ID 2:
![image.png](images/worker2.png) 

Worker ID 3:
![image.png](images/worker3.png) 

Worker ID 4:
![image.png](images/worker4.png) 

Worker ID 5:
![image.png](images/worker5.png) 

Worker ID 6:
![image.png](images/worker6.png) 

Worker ID 7:
![image.png](images/worker7.png) 

Worker ID 8:
![image.png](images/worker8.png) 

Worker ID 9:
![image.png](images/worker9.png) 

Worker ID 10:
![image.png](images/worker10.png) 

Master:
![image.png](images/master.png)

Test Result:
![image.png](images/test_result.png)
Total 320000000 records are correctly sorted. (1000 input files in each macine * 32000 record * 10 machine)
