import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._
import java.io.{File,PrintWriter}
import scala.collection.mutable.ListBuffer
import java.nio.file.Paths
import scala.util.Random


class Merge() {
  private val numPartitions = 50
  private val numSample = 10000
  /* divide given files by the numPartitions
   * @param inputPaths: List[String] / the input file paths
   * @param outputDir: String / the directory that divided file would saved
   * @return newFilePaths: List[String]
   */
  def divideFiles(inputPaths: List[String], outputDir : String): List[ListBuffer[String]] ={
    val sortedKeys = getSample(inputPaths).sorted
    val range = getRange(sortedKeys)

    val inputDataList = readFile(inputPaths)
    val fileDataAndPath = inputDataList.zip(inputPaths)
    val newFilePaths = List.fill(numPartitions)(ListBuffer[String]())

    try {
      fileDataAndPath.foreach { case ((fileData, fileSize), filePath) =>
        assert(fileData.nonEmpty)
        val dividedData = Array.fill(numPartitions)(ListBuffer[String]())
        fileData.foreach { line =>
          val key = line.take(10)
          val idx = range.indexWhere(rangeValue => rangeValue > key)
          val partIdx = idx match{
            case -1 => numPartitions -1
            case _ => idx
          }
          dividedData(partIdx) += line
        }

        dividedData.zipWithIndex.foreach { case(data, partIdx)=>
          val fileName = Paths.get(filePath).getFileName.toString
          val fileDir = outputDir+s"part_${partIdx}/"
          val newFilePath = fileDir + fileName
          makeDir(fileDir)

          newFilePaths(partIdx) += newFilePath
          val writer = new PrintWriter(newFilePath)

          val dividedDataList = data.toList
          try {
            dividedDataList.foreach {writer.println}
          } finally {
            writer.close()
            //println(s"finished writing to $newFilePath")
          }
        }
      }
    } catch {
      case ex: Exception =>
        println(s"some Exception while dividing Files: ${ex.getMessage}")
    }
    newFilePaths
  }

  /* read the files and return the fileData and lineCnt
   * fileData: Iterator[String] / the file's data
   * lineCnt: Int / the number of lines in the file
   * @param filePaths: List[String] / the paths of the file to open
   * @return fileDataAndSize : List[(fileData, lineCnt)]
   */
  private def readFile(filePaths: List[String]): List[(Iterator[String], Int)] = {
    filePaths.map { filePath =>
      val source = Source.fromFile(filePath)
      val source2 = Source.fromFile(filePath)
      try{
        val lineCnt = source2.getLines().length
        val fileData = source.getLines()
        (fileData, lineCnt)
      } catch{
        case ex:Exception =>
          println(s"Error during read file : ${ex.getMessage}")
          (Iterator.empty, 0)
      }
    }
  }
  /* get the samples with given inputDataList randomly
   * fileData: Iterator[String] / the file's data
   * lineCnt: Int / the number of lines in the file
   * inputDataList: List[(fileData, lineCnt)] / the input data
   * @param inputPaths: List[String] / the file paths
   * @return samples : List[(fileData, lineCnt)]
   */
  private def getSample(inputPaths: List[String]): List[String] = {
    //println("getSample started")
    val inputDataList = readFile(inputPaths)
    assert(inputDataList.nonEmpty)

    val sampleNumForEachFile = numSample/inputDataList.length
    val keySample = ListBuffer[String]()
    inputDataList.foreach { case (fileData,lineCnt) =>
      val randomIndexes = getRandomIndexes(sampleNumForEachFile, lineCnt)
      var dropLineCount = 0
      randomIndexes.foreach { idx =>
        val diff = idx - dropLineCount -1
        val sampledLine = fileData.drop(diff).next()
        dropLineCount = idx
        keySample += sampledLine.take(10)
        //println(s"key : ${sampledLine.take(10)} at idx : $idx")
      }
    }
    //println(s"total sample num : ${keySample.length}")
    assert(keySample.nonEmpty)
    keySample.toList
  }
  /* get the random numbers and return
   * @param sampleNum: Int / the number of indexes
   * @param lineCnt: Int / the range of indexes
   * idx : the index
   * @return randomIndexes : IndexedSeq[Int]
   */
  private def getRandomIndexes(sampleNum: Int, lineCnt: Int): IndexedSeq[Int] = {
    val random = new Random()
    (0 until sampleNum).map { _ =>
      random.nextInt(lineCnt)
    }.sorted
  }

  /* get the partition range with given sortedKeys
   * @param sortedKeys: List[String] / the sorted keys
   * @return range: List[String]
   */
  def getRange(sortedKeys: List[String]): List[String] = {
    val sampleSize = sortedKeys.length
    val rangeSize = sampleSize/numPartitions
    try{
      (0 until numPartitions).map { i=>
        val idx = if(i+1 == numPartitions) sampleSize -1 else (i+1)*rangeSize -1
        sortedKeys(idx)
      }.toList.sorted
    } catch {
      case ex: Exception =>
        println(s"Error during range calculation: ${ex.getMessage}")
        List.empty
    }
  }

  /* make Directory if it does not exists
   * @param fileDir: String / the file directory path
   * @return dir: File //unused
   */
  private def makeDir(fileDir: String): File = {
    val dir = new File(fileDir)
    if(!dir.exists()) {
      dir.mkdirs()
      println("directory created" + s"$fileDir")
    }
    dir
  }

  def merge(inputPaths: List[String], outputDir: String): List[String] = {
    val dividedFilePathList = divideFiles(inputPaths, outputDir)
    println("done partitioning")

    val outputPathList = ListBuffer[String]()
    dividedFilePathList.zipWithIndex.foreach { case (partPaths, partIdx) =>
      val outputPath = outputDir+s"part_${partIdx}_sorted.txt"
      val mergedDataFuture = Future { divideAndMerge(partPaths.toList) }
      val finalData = Await.result(mergedDataFuture,Duration.Inf)

      val writer = new PrintWriter(outputPath)
      try{
        finalData.foreach(writer.println)
        outputPathList += outputPath
      } finally {
        writer.close()
      }

    }
    closeAndDeleteFile(dividedFilePathList)
    concatFile(outputPathList.toList,outputDir)
    outputPathList.toList
  }

  private def mergeTwoItr(data1: Iterator[String], data2: Iterator[String]): Iterator[String] = {
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

  private def divideAndMerge(filePathsList: List[String]): Iterator[String] = {
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
      val firstFuture = Future { divideAndMerge(firstHalf) }
      val secondFuture = Future { divideAndMerge(secondHalf) }

      //Future를 Await 하고, 결과로 나온 값 두개를 mergeTwoLists()
      val data1 = Await.result(firstFuture, Duration.Inf)
      val data2 = Await.result(secondFuture, Duration.Inf)
      mergeTwoItr(data1, data2)
    }
  }
  private def closeAndDeleteFile(filePaths: List[ListBuffer[String]]): Unit = {
    filePaths.flatMap(_.toList).foreach{ filePath =>
      try {
        val file = new File(filePath)
        Source.fromFile(filePath).close()
        file.delete()
      } catch {
        case ex:Exception =>
          println(s"some Error during close and delete file : ${ex.getMessage}")
      }
    }
  }

  private def concatFile(filePath: List[String], outputDir: String): Unit = {
    val writer = new PrintWriter(outputDir+"sorted.txt")
    filePath.foreach{ path =>
      val source = Source.fromFile(path)
      val lines = source.getLines()
      lines.foreach{writer.println}
      source.close()
    }
    writer.close()
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
