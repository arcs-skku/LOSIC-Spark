/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import scala.collection.JavaConverters._
<<<<<<< HEAD
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, LinkedHashSet, HashMap}
=======
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, LinkedHashSet, HashMap, HashSet}
>>>>>>> LOSIC

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util._


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
 */
@DeveloperApi
class TaskMetrics private[spark] () extends Serializable with Logging {
  // Each metric is internally represented as an accumulator
  private val _executorDeserializeTime = new LongAccumulator
  private val _executorDeserializeCpuTime = new LongAccumulator
  private val _executorRunTime = new LongAccumulator
  private val _executorCpuTime = new LongAccumulator
  private val _resultSize = new LongAccumulator
  private val _jvmGCTime = new LongAccumulator
  private val _resultSerializationTime = new LongAccumulator
  private val _memoryBytesSpilled = new LongAccumulator
  private val _diskBytesSpilled = new LongAccumulator
  private val _peakExecutionMemory = new LongAccumulator
  private val _updatedBlockStatuses = new CollectionAccumulator[(BlockId, BlockStatus)]
  
  // SSPARK part
  private val _updatedBlockTime = new CollectionAccumulator[(Int, Long)]
  private val _updatedBlockSize = new CollectionAccumulator[(Int, Long)]
  
  private[spark] def toJavaList(v: LinkedHashMap[Int, Long]): java.util.List[(Int, Long)] = {
    v.toList.asJava
  }
  
  def updatedBlockTime: Seq[(Int, Long)] = {
    _updatedBlockTime.value.asScala
  }
<<<<<<< HEAD
  def updatedBlockSize: Seq[(Int, Long)] = {
    _updatedBlockSize.value.asScala
  }
  
=======
  
  def updatedBlockSize: Seq[(Int, Long)] = {
    _updatedBlockSize.value.asScala
  }

  // for profiling ratio
  private[spark] def toJavaList(v: HashSet[Int]): java.util.List[Int] = {
    v.toList.asJava
  }

  private val _updatedComputedRdds = new CollectionAccumulator[Int]  

  def updatedComputedRdds: Seq[Int] = { 
    _updatedComputedRdds.value.asScala
  }

  private[spark] def setUpdatedComputedRdds(v: java.util.List[Int]): Unit =
    _updatedComputedRdds.setValue(v)

  private val _computedRdds = new HashSet[Int]

  def computedRdds: HashSet[Int] = _computedRdds


>>>>>>> LOSIC
  private[spark] def setUpdatedBlockTime(v: java.util.List[(Int, Long)]): Unit =
    _updatedBlockTime.setValue(v)
  private[spark] def setUpdatedBlockSize(v: java.util.List[(Int, Long)]): Unit =
    _updatedBlockSize.setValue(v)
<<<<<<< HEAD
 
=======
  
>>>>>>> LOSIC
  /* SSPARK: get BlockInfos from RDD iterator
   *  blockTime use LinkedHashSet for guarantee the sequence of time stamps
   *  As a result, it can get actual block computing time on Executor.run 
   */
  class BlockInfos(val _time: Long, 
                   val _isStart: Boolean,
                   val _rddId: Int, 
                   val _partitionId: Int, 
                   val _isCached: Boolean,
                   val _root: Int                   
                   ) {
    val time = _time
    val isStart = _isStart
    val rddId = _rddId
    val partitionId = _partitionId
    val isCached = _isCached
    val root = _root

    override def toString(): String = {
      s"rdd[p]:${rddId}[${partitionId}] root:${root} isStart:${isStart} isCached:${isCached} time:${time}"
    }
  }

  private val _blockTime = new LinkedHashSet[BlockInfos]
  private val _blockSize = new LinkedHashMap[Int, Long] // it should keep the ordering to devide into input/output size
  
  def blockTime: LinkedHashSet[BlockInfos] = _blockTime
  def blockSize: LinkedHashMap[Int, Long]  = _blockSize

  

  private [spark] def setBlockTime(_time: Long, 
                   _isStart: Boolean,
                   _rddId: Int, 
                   _partitionId: Int, 
                   _isCached: Boolean,
                   _root: Int
                   ): Unit =
    _blockTime.add(new BlockInfos(_time, _isStart, _rddId, _partitionId, _isCached, _root))
  
  private [spark] def setBlockTime(infos: BlockInfos): Unit = 
    _blockTime.add(infos)

  private [spark] def setBlockSize(_rddId: Int, _size: Long): Unit =
    _blockSize.put(_rddId, _size)
   
  def calcBlockTime(isSSparkLogEnabled: Boolean, taskId: Long): LinkedHashMap[Int, Long] ={
    val calc = new LinkedHashMap[Int, Long]
    var startTime = 0L
    var endTime = 0L
    blockTime.foreach{ x =>
      if (x.isStart) {
        startTime = x.time  // doesn't need to distinguish root, just overwrite a startTime for finding last attached node
        /*
        if (x.rddId == x.root)  // no cached, run from the root of lineage
          startTime = x.time
        else  // scheduled start block but it's not actually executed because of lazy execution
          None
          */
      }
      else { //isEnd
        if (x.isCached)
          startTime = x.time
        else {
          endTime = x.time
          if (endTime == 0L || startTime == 0L)
            logErrorSSP(s"Time got 0 for rdd ${x.rddId} with root ${x.root}, ST=${startTime}, ET=${endTime}")
          else {
            logInfoSSP(s"Task ${taskId} rdd ${x.rddId} from ${startTime} to ${endTime}", isSSparkLogEnabled)
            calc.put(x.rddId, endTime - startTime)
          }
          startTime = x.time
        }
      }
    }
    calc
  }
  // END SSPARK

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.sum

  /**
   * CPU Time taken on the executor to deserialize this task in nanoseconds.
   */
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime.sum

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  def executorRunTime: Long = _executorRunTime.sum

  /**
   * CPU Time the executor spends actually running the task
   * (including fetching shuffle data) in nanoseconds.
   */
  def executorCpuTime: Long = _executorCpuTime.sum

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.sum

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.sum

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.sum

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.sum

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.sum

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.sum

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   *
   * Tracking the _updatedBlockStatuses can use a lot of memory.
   * It is not used anywhere inside of Spark so we would ideally remove it, but its exposed to
   * the user in SparkListenerTaskEnd so the api is kept for compatibility.
   * Tracking can be turned off to save memory via config
   * TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = {
    // This is called on driver. All accumulator updates have a fixed value. So it's safe to use
    // `asScala` which accesses the internal values using `java.util.Iterator`.
    _updatedBlockStatuses.value.asScala
  }

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorDeserializeCpuTime(v: Long): Unit =
    _executorDeserializeCpuTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
  private[spark] def setExecutorCpuTime(v: Long): Unit = _executorCpuTime.setValue(v)
  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit =
    _updatedBlockStatuses.add(v)
  private[spark] def setUpdatedBlockStatuses(v: java.util.List[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)
  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v.asJava)

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   */
  val inputMetrics: InputMetrics = new InputMetrics()

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  val outputMetrics: OutputMetrics = new OutputMetrics()

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  val shuffleReadMetrics: ShuffleReadMetrics = new ShuffleReadMetrics()

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  val shuffleWriteMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  /**
   * A list of [[TempShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[TempShuffleReadMetrics]]
   * for each dependency and merge these metrics before reporting them to the driver.
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[TempShuffleReadMetrics]

  /**
   * Create a [[TempShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   */
  private[spark] def createTempShuffleReadMetrics(): TempShuffleReadMetrics = synchronized {
    val readMetrics = new TempShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics)
    }
  }

  // Only used for test
  private[spark] val testAccum = sys.props.get("spark.testing").map(_ => new LongAccumulator)


  import InternalAccumulator._
  @transient private[spark] lazy val nameToAccums = LinkedHashMap(
    EXECUTOR_DESERIALIZE_TIME -> _executorDeserializeTime,
    EXECUTOR_DESERIALIZE_CPU_TIME -> _executorDeserializeCpuTime,
    EXECUTOR_RUN_TIME -> _executorRunTime,
    EXECUTOR_CPU_TIME -> _executorCpuTime,
    RESULT_SIZE -> _resultSize,
    JVM_GC_TIME -> _jvmGCTime,
    RESULT_SERIALIZATION_TIME -> _resultSerializationTime,
    MEMORY_BYTES_SPILLED -> _memoryBytesSpilled,
    DISK_BYTES_SPILLED -> _diskBytesSpilled,
    PEAK_EXECUTION_MEMORY -> _peakExecutionMemory,
    UPDATED_BLOCK_STATUSES -> _updatedBlockStatuses,
<<<<<<< HEAD
    BLOCK_TIME -> _updatedBlockTime,    //SSPARK
    BLOCK_SIZE -> _updatedBlockSize,    //SSPARK
=======
    BLOCK_TIME -> _updatedBlockTime,       //SSPARK
    BLOCK_SIZE -> _updatedBlockSize,       //SSPARK
    COMPUTED_RDDS -> _updatedComputedRdds, //SSPARK
>>>>>>> LOSIC
    shuffleRead.REMOTE_BLOCKS_FETCHED -> shuffleReadMetrics._remoteBlocksFetched,
    shuffleRead.LOCAL_BLOCKS_FETCHED -> shuffleReadMetrics._localBlocksFetched,
    shuffleRead.REMOTE_BYTES_READ -> shuffleReadMetrics._remoteBytesRead,
    shuffleRead.REMOTE_BYTES_READ_TO_DISK -> shuffleReadMetrics._remoteBytesReadToDisk,
    shuffleRead.LOCAL_BYTES_READ -> shuffleReadMetrics._localBytesRead,
    shuffleRead.FETCH_WAIT_TIME -> shuffleReadMetrics._fetchWaitTime,
    shuffleRead.RECORDS_READ -> shuffleReadMetrics._recordsRead,
    shuffleWrite.BYTES_WRITTEN -> shuffleWriteMetrics._bytesWritten,
    shuffleWrite.RECORDS_WRITTEN -> shuffleWriteMetrics._recordsWritten,
    shuffleWrite.WRITE_TIME -> shuffleWriteMetrics._writeTime,
    input.BYTES_READ -> inputMetrics._bytesRead,
    input.RECORDS_READ -> inputMetrics._recordsRead,
    output.BYTES_WRITTEN -> outputMetrics._bytesWritten,
    output.RECORDS_WRITTEN -> outputMetrics._recordsWritten
  ) ++ testAccum.map(TEST_ACCUM -> _)

  @transient private[spark] lazy val internalAccums: Seq[AccumulatorV2[_, _]] =
    nameToAccums.values.toIndexedSeq

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def register(sc: SparkContext): Unit = {
    nameToAccums.foreach {
      case (name, acc) => acc.register(sc, name = Some(name), countFailedValues = true)
    }
  }

  /**
   * External accumulators registered with this task.
   */
  @transient private[spark] lazy val externalAccums = new ArrayBuffer[AccumulatorV2[_, _]]

  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    externalAccums += a
  }

  private[spark] def accumulators(): Seq[AccumulatorV2[_, _]] = internalAccums ++ externalAccums

  private[spark] def nonZeroInternalAccums(): Seq[AccumulatorV2[_, _]] = {
    // RESULT_SIZE accumulator is always zero at executor, we need to send it back as its
    // value will be updated at driver side.
    internalAccums.filter(a => !a.isZero || a == _resultSize)
  }
}


private[spark] object TaskMetrics extends Logging {
  import InternalAccumulator._

  /**
   * Create an empty task metrics that doesn't register its accumulators.
   */
  def empty: TaskMetrics = {
    val tm = new TaskMetrics
    tm.nameToAccums.foreach { case (name, acc) =>
      acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), true)
    }
    tm
  }

  def registered: TaskMetrics = {
    val tm = empty
    tm.internalAccums.foreach(AccumulatorContext.register)
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of [[AccumulableInfo]], called on driver only.
   * The returned [[TaskMetrics]] is only used to get some internal metrics, we don't need to take
   * care of external accumulator info passed in.
   */
  def fromAccumulatorInfos(infos: Seq[AccumulableInfo]): TaskMetrics = {
    val tm = new TaskMetrics
    infos.filter(info => info.name.isDefined && info.update.isDefined).foreach { info =>
      val name = info.name.get
      val value = info.update.get
      if (name == UPDATED_BLOCK_STATUSES) {
        tm.setUpdatedBlockStatuses(value.asInstanceOf[java.util.List[(BlockId, BlockStatus)]])
      } 
      /*
      else if (name == BLOCK_TIME){
        tm.setUpdatedBlockTime(value.asInstanceOf[java.util.List[(Int, Long)]])
      }
      else if (name == BLOCK_SIZE){
        tm.setUpdatedBlockSize(value.asInstanceOf[java.util.List[(Int, Long)]])
      }*/
<<<<<<< HEAD
      else if (name == BLOCK_TIME || name == BLOCK_SIZE) {
=======
      else if (name == BLOCK_TIME || name == BLOCK_SIZE || name == COMPUTED_RDDS) {
>>>>>>> LOSIC
        None
      }
      else {
        tm.nameToAccums.get(name).foreach(
          _.asInstanceOf[LongAccumulator].setValue(value.asInstanceOf[Long])
        )
      }
    }
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   */
  def fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics = {
    val tm = new TaskMetrics
    for (acc <- accums) {
      val name = acc.name
      if (name.isDefined && tm.nameToAccums.contains(name.get)) {
        val tmAcc = tm.nameToAccums(name.get).asInstanceOf[AccumulatorV2[Any, Any]]
        tmAcc.metadata = acc.metadata
        tmAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
      } else {
        tm.externalAccums += acc
      }
    }
    tm
  }
}
