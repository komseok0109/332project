# gRPC

JDK : 11

SDK : 2.13.15

SBT : 1.8.3

- build.sbt
    - 추가

```scala
Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)

libraryDependencies ++= Seq(
  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)
```

- project/scalapb.sbt
    - 생성

```scala
**addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"**
```

- src/main/protobuf/hello.proto
    - 생성

```scala
syntax = "proto3";

package hello;

service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply) {}
}

message HelloRequest {
  string name = 1;
}

message HelloReply {
  string message = 1;
}

```

- sbt실행
    - compile입력
    - .idea 디렉토리 삭제
    - intellij 재부팅

- src/main/scala/GreeterServer.scala
    - 생성

```scala
import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Future}
import hello.hello._ //targer/scala-2.13/classes에서 모듈 위치

object GreeterServer {
  def main(args: Array[String]): Unit = {
    val server = new GreeterServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }
}

class GreeterServer(executionContext: ExecutionContext) { self =>
  private[this] var server: io.grpc.Server = null

  def start(): Unit = {
    server = ServerBuilder.forPort(50051)
      .addService(GreeterGrpc.bindService(new GreeterImpl, executionContext))
      .build()
      .start()
    println("Server started, listening on " + 50051)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  class GreeterImpl extends GreeterGrpc.Greeter {
    override def sayHello(req: HelloRequest): Future[HelloReply] = {
      val reply = HelloReply(message = "Hello " + req.name)
      Future.successful(reply)
    }
  }
}

```

- src/main/scala/GreeterClient.scala
    - 생성

```scala
import io.grpc.ManagedChannelBuilder
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import hello.hello._ //targer/scala-2.13/classes에서 모듈 위치

object GreeterClient {
  def main(args: Array[String]): Unit = {
    val channel = ManagedChannelBuilder.forAddress("localhost", 50051)
      .usePlaintext()
      .build()

    val stub = GreeterGrpc.stub(channel)

    val request = HelloRequest(name = "World")
    val responseFuture: Future[HelloReply] = stub.sayHello(request)

    val response = Await.result(responseFuture, 5.seconds)
    println("Greeting: " + response.message)

    channel.shutdownNow()
  }
}

```

- 동작 방법1
    - intellij 맨 실행 왼쪽 누르고 아래칸 Edit Configurations
    - 어플리케이션 추가
        - server : 클래스 생성 && client  : 클래스 생성

- 동작 방법2
    - sbt에서 runMain으로 object실행(둘 중 하나만 가능, 나머지 하나는 실행버튼으로 실행)
        - runMain GreeterServer
        - runMain GreeterClient

![image.png](images/gRPC1.png)

![image.png](images/gRPC2.png)

- 서버 실행

![image.png](images/gRPC3.png)

- 클라이언트 실행 (서버 실행 상태에서)

![image.png](images/gRPC4.png)

출처: [https://scalapb.github.io/docs/grpc/](https://scalapb.github.io/docs/grpc/)

- 코드 : [shinjw4929/cs332_project_gRPC at gRPC_test_1](https://github.com/shinjw4929/cs332_project_gRPC/tree/gRPC_test_1)