import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._
import java.io.{File,PrintWriter, FileWriter, BufferedWriter}
import scala.collection.mutable.ListBuffer
import java.nio.file.Paths
import scala.util.Random


class Merge() {
  val numPartitions = 4
  val numSample = 10000
  def partitionFiles(inputPaths: List[String], outputPath : String): List[ListBuffer[String]] ={
    val sortedKeys = getSample(readFile(inputPaths)).sorted
    //println("got samples")
    val rangeSize = numSample/numPartitions
    var range = List[String]()
    try{
      range = (0 until numPartitions).map { i=>
        val idx = if(i+1 == numPartitions) numSample -1 else (i+1)*rangeSize -1
        println(s"idx: $idx, value: ${sortedKeys(idx)}")
        sortedKeys(idx)
      }.toList.sorted
    } catch {
      case ex: Exception =>
        println(s"Error during range calculation: ${ex.getMessage}")
    }
    println(range)
    println("got range")


    val inputDataList = readFile(inputPaths)
    val fileDataAndPath = inputDataList.zip(inputPaths)
    val newFilePaths = List.fill(numPartitions)(ListBuffer[String]())

    try {
      fileDataAndPath.foreach { case (fileData, filePath) =>
        assert(fileData.nonEmpty)
        val partitionedData = Array.fill(numPartitions)(ListBuffer[String]())
        fileData.foreach { line =>
          val key = line.take(10)
          val idx = range.indexWhere(rangeValue => rangeValue > key)
          val partIdx = idx match{
            case -1 => numPartitions -1
            case _ => idx
          }
          partitionedData(partIdx) += line
        }

        partitionedData.zipWithIndex.foreach { case(buffer, partIdx)=>
          val fileName = Paths.get(filePath).getFileName.toString
          val fileDir = outputPath+s"${partIdx}/"

          val dir = new File(fileDir)
          if(!dir.exists()){
            dir.mkdirs()
            println("directory created" + s"$fileDir")
          }
          val path = fileDir + fileName
          newFilePaths(partIdx) += path
          val writer = new PrintWriter(path)

          val dataList = buffer.toList
          try {
            dataList.foreach {writer.println}
          } finally {
            writer.close()
            //println(s"finished writing to $path")
          }
        }
      }
    } catch {
      case ex: Exception =>
        println(s"some Exception while partitioning Files: ${ex.getMessage}")
    }
    newFilePaths
  }

  def readFile(filePaths: List[String]): List[Iterator[String]] = {
    filePaths.map { filePath =>
      val source = Source.fromFile(filePath)
      try{
        source.getLines()
      } catch{
        case ex:Exception =>
          println(s"Error during read file : ${ex.getMessage}")
          Iterator.empty
      }
    }
  }

  def getSample(inputDataList: List[Iterator[String]]): List[String] = {
    //println("getting samples")
    val totalSampleSize = numSample
    val sampleNumForEachFile = totalSampleSize/inputDataList.length
    val random = new Random()
    val sampleKey = ListBuffer[String]()
    inputDataList.foreach { fileData =>
      // Get a random sample from the LazyList
      val randomIndexes = (0 until sampleNumForEachFile).map { _ =>
        random.nextInt(sampleNumForEachFile)
      }.toList
      //println(randomIndexes)
      randomIndexes.foreach { idx =>
        val sampledLine = fileData.drop(idx).next()
        sampleKey += sampledLine.take(10)
      }
    }
    assert(sampleKey.nonEmpty)
    sampleKey.toList
  }

  /*
  def splitRange(startKey: String, endKey: String): List[String] = {
    println("start splitRange")
    try{
      val start = BigInt(startKey)
      val end = BigInt(endKey)
      val rangeLen = end - start
      val intervalSize = rangeLen/numPartitions
      val splitPoints = (0 until numPartitions).map { i =>
        val splitPoint = start + (i + intervalSize)
        splitPoint.toString
      }
      println("splitRange succeed")
      splitPoints.toList
    } catch {
      case e: NumberFormatException =>
        println(s"Error during splitRange: Invalid number format in start or end key. ${e.getMessage}")
        List() // Return an empty list or some default value on error
      case e: IllegalArgumentException =>
        println(s"Error during splitRange: ${e.getMessage}")
        List() // Return an empty list or handle appropriately
      case e: Exception =>
        println(s"Unexpected error during splitRange: ${e.getMessage}")
        List() // Return an empty list or handle appropriately
    }
  }

   */

  def merge(inputPaths: List[String], myRange: (Int, (String,String)), outputDir: String): List[String] = {
    val (workerId, (startKey, endKey)) = myRange
    val inputPathList = partitionFiles(inputPaths, outputDir)
    println("done partitioning")
    val outputPathList = ListBuffer[String]()
    inputPathList.zipWithIndex.foreach { case (partPathList, partIdx) =>
      val outputPath = outputDir+s"${partIdx}/"+s"sorted.txt"
      val finalData = divideFiles(partPathList.toList)

      val writer = new PrintWriter(outputPath)
      try{
        finalData.foreach(writer.println)
        outputPathList += outputPath
      } finally {
        writer.close()
      }
    }
    outputPathList.toList
  }

  def mergeTwoItr(data1: Iterator[String], data2: Iterator[String]): Iterator[String] = {
    val buffer = ListBuffer[String]()
    val it1 = data1.buffered
    val it2 = data2.buffered

    while (it1.hasNext && it2.hasNext) {
      if (it1.head <= it2.head) buffer += it1.next()
      else buffer += it2.next()
    }

    buffer ++= it1
    buffer ++= it2
    buffer.iterator
  }

  def divideFiles(filePathsList: List[String]): Iterator[String] = {
    if (filePathsList.size <= 1) {
      Source.fromFile(filePathsList.head).getLines()
    }
    else if (filePathsList.size == 2) {
      //비교할 파일이 2개라면 mergeTwoList로 합치기
      val data1 = Source.fromFile(filePathsList.head).getLines()
      val data2 = Source.fromFile(filePathsList(1)).getLines()
      mergeTwoItr(data1, data2)
    }
    else {
      val (firstHalf, secondHalf) = filePathsList.splitAt(filePathsList.size / 2)
      val firstFuture = Future { divideFiles(firstHalf) }
      val secondFuture = Future { divideFiles(secondHalf) }

      //Future를 Await 하고, 결과로 나온 값 두개를 mergeTwoLists()
      val data1 = Await.result(firstFuture, Duration.Inf)
      val data2 = Await.result(secondFuture, Duration.Inf)
      mergeTwoItr(data1, data2)
    }
  }
  private def isDataSorted(data: Iterator[String]): Boolean = {
    val keys = data.map(_.take(10))
    keys.sliding(2).forall {
      case Seq(prev, curr) => prev <= curr
      case _               => true
    }
  }

  def isFileSorted(fileName: String): Boolean = {
    try {
      // 파일을 읽어 List[String]으로 변환
      val source = Source.fromFile(fileName)
      val isSorted = isDataSorted(source.getLines())
      source.close()
      isSorted
    } catch {
      case ex: Exception =>
        println(s"no file exist: ${ex.getMessage}")
        false
    }
  }
}
