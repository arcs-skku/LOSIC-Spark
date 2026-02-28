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

package org.apache.spark

import java.nio.file.{Files, Paths} //SSPARK
import java.io._
import java.net.URI
import java.util.{Arrays, Locale, Properties, ServiceLoader, UUID}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.io.Source //SSPARK
import scala.util.control.Breaks.{break, breakable} //SSPARK
import scala.collection.mutable.{LinkedHashMap, LinkedHashSet, HashSet} //SSPARK
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.generic.Growable
import scala.collection.mutable.HashMap
import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal

import com.google.common.collect.MapMaker
import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.spark.input.{FixedLengthBinaryInputFormat, PortableDataStream, StreamInputFormat, WholeTextFileInputFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.partial.{ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, StandaloneSchedulerBackend}
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.ThreadStackTrace
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.TriggerThreadDump
import org.apache.spark.ui.{ConsoleProgressBar, SparkUI}
import org.apache.spark.util._

/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 *
 * Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
 * creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.
 *
 * @param config a Spark Config object describing the application configuration. Any settings in
 *   this config overrides the default configs as well as system properties.
 */
class SparkContext(config: SparkConf) extends Logging {

  // The call site where this SparkContext was constructed.
  private val creationSite: CallSite = Utils.getCallSite()

  // If true, log warnings instead of throwing exceptions when multiple SparkContexts are active
  private val allowMultipleContexts: Boolean =
    config.getBoolean("spark.driver.allowMultipleContexts", false)

  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having started construction.
  // NOTE: this must be placed at the beginning of the SparkContext constructor.
  SparkContext.markPartiallyConstructed(this, allowMultipleContexts)

  val startTime = System.currentTimeMillis()

  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)

  private[spark] def assertNotStopped(): Unit = {
    if (stopped.get()) {
      val activeContext = SparkContext.activeContext.get()
      val activeCreationSite =
        if (activeContext == null) {
          "(No active SparkContext.)"
        } else {
          activeContext.creationSite.longForm
        }
      throw new IllegalStateException(
        s"""Cannot call methods on a stopped SparkContext.
           |This stopped SparkContext was created at:
           |
           |${creationSite.longForm}
           |
           |The currently active SparkContext was created at:
           |
           |$activeCreationSite
         """.stripMargin)
    }
  }

  /**
   * Create a SparkContext that loads settings from system properties (for instance, when
   * launching with ./bin/spark-submit).
   */
  def this() = this(new SparkConf())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(SparkContext.updatedConf(conf, master, appName))

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes.
   */
  def this(
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map()) = {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
  }

  // The following constructors are required when Java code accesses SparkContext directly.
  // Please see SI-4278

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   */
  private[spark] def this(master: String, appName: String) =
    this(master, appName, null, Nil, Map())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   */
  private[spark] def this(master: String, appName: String, sparkHome: String) =
    this(master, appName, sparkHome, Nil, Map())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
    this(master, appName, sparkHome, jars, Map())

  // log out Spark Version in Spark driver log
  logInfo(s"Running Spark version $SPARK_VERSION")

  /* ------------------------------------------------------------------------------------- *
   | Private variables. These variables keep the internal state of the context, and are    |
   | not accessible by the outside world. They're mutable since we want to initialize all  |
   | of them to some neutral value ahead of time, so that calling "stop()" while the       |
   | constructor is still running is safe.                                                 |
   * ------------------------------------------------------------------------------------- */

  private var _conf: SparkConf = _
  private var _eventLogDir: Option[URI] = None
  private var _eventLogCodec: Option[String] = None
  private var _listenerBus: LiveListenerBus = _
  private var _env: SparkEnv = _
  private var _statusTracker: SparkStatusTracker = _
  private var _progressBar: Option[ConsoleProgressBar] = None
  private var _ui: Option[SparkUI] = None
  private var _hadoopConfiguration: Configuration = _
  private var _executorMemory: Int = _
  private var _schedulerBackend: SchedulerBackend = _
  private var _taskScheduler: TaskScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  @volatile private var _dagScheduler: DAGScheduler = _
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _eventLogger: Option[EventLoggingListener] = None
  private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
  private var _cleaner: Option[ContextCleaner] = None
  private var _listenerBusStarted: Boolean = false
  private var _jars: Seq[String] = _
  private var _files: Seq[String] = _
  private var _shutdownHookRef: AnyRef = _
  private var _statusStore: AppStatusStore = _

  /* ------------------------------------------------------------------------------------- *
   | Accessors and public fields. These provide access to the internal state of the        |
   | context.                                                                              |
   * ------------------------------------------------------------------------------------- */

  // SSPARK profiling dir
  val profilingPath = Paths.get(sys.env("SPARK_HOME") + "/profiling")
  if (!Files.exists(profilingPath)){
    logInfoSSP("Doesn't exist profilng dir, make dir")
    Files.createDirectory(profilingPath)
  }
  val profilingDir: String = profilingPath.toString
  
  val rdgPath = Paths.get(sys.env("SPARK_HOME") + "/rdg")
  if (!Files.exists(rdgPath)){
    logInfoSSP("Doesn't exist RDG dir, make dir")
    Files.createDirectory(rdgPath)
  }
  val rdgDir: String = rdgPath.toString
  
  private[spark] def conf: SparkConf = _conf

  /**
   * Return a copy of this SparkContext's configuration. The configuration ''cannot'' be
   * changed at runtime.
   */
  def getConf: SparkConf = conf.clone()

  def jars: Seq[String] = _jars
  def files: Seq[String] = _files
  def master: String = _conf.get("spark.master")
  def deployMode: String = _conf.getOption("spark.submit.deployMode").getOrElse("client")
  def appName: String = _conf.get("spark.app.name")

  private[spark] def isEventLogEnabled: Boolean = _conf.getBoolean("spark.eventLog.enabled", false)
  private[spark] def eventLogDir: Option[URI] = _eventLogDir
  private[spark] def eventLogCodec: Option[String] = _eventLogCodec

  def isLocal: Boolean = Utils.isLocalMaster(_conf)

  /**
   * @return true if context is stopped or in the midst of stopping.
   */
  def isStopped: Boolean = stopped.get()

  private[spark] def statusStore: AppStatusStore = _statusStore

  // An asynchronous listener bus for Spark events
  private[spark] def listenerBus: LiveListenerBus = _listenerBus

  // This function allows components created by SparkEnv to be mocked in unit tests:
  private[spark] def createSparkEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus): SparkEnv = {
    SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master, conf))
  }

  private[spark] def env: SparkEnv = _env

  // Used to store a URL for each static file/jar together with the file's local timestamp
  private[spark] val addedFiles = new ConcurrentHashMap[String, Long]().asScala
  private[spark] val addedJars = new ConcurrentHashMap[String, Long]().asScala

  // Keeps track of all persisted RDDs
  private[spark] val persistentRdds = {
    val map: ConcurrentMap[Int, RDD[_]] = new MapMaker().weakValues().makeMap[Int, RDD[_]]()
    map.asScala
  }
  def statusTracker: SparkStatusTracker = _statusTracker

  private[spark] def progressBar: Option[ConsoleProgressBar] = _progressBar

  private[spark] def ui: Option[SparkUI] = _ui

  def uiWebUrl: Option[String] = _ui.map(_.webUrl)

  /**
   * A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
   *
   * @note As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
   * plan to set some global configurations for all Hadoop RDDs.
   */
  def hadoopConfiguration: Configuration = _hadoopConfiguration

  private[spark] def executorMemory: Int = _executorMemory

  // Environment variables to pass to our executors.
  private[spark] val executorEnvs = HashMap[String, String]()

  // Set SPARK_USER for user who is running SparkContext.
  val sparkUser = Utils.getCurrentUserName()

  private[spark] def schedulerBackend: SchedulerBackend = _schedulerBackend

  private[spark] def taskScheduler: TaskScheduler = _taskScheduler
  private[spark] def taskScheduler_=(ts: TaskScheduler): Unit = {
    _taskScheduler = ts
  }

  private[spark] def dagScheduler: DAGScheduler = _dagScheduler
  private[spark] def dagScheduler_=(ds: DAGScheduler): Unit = {
    _dagScheduler = ds
  }

  /**
   * A unique identifier for the Spark application.
   * Its format depends on the scheduler implementation.
   * (i.e.
   *  in case of local spark app something like 'local-1433865536131'
   *  in case of YARN something like 'application_1433865536131_34483'
   *  in case of MESOS something like 'driver-20170926223339-0001'
   * )
   */
  def applicationId: String = _applicationId
  def applicationAttemptId: Option[String] = _applicationAttemptId

  private[spark] def eventLogger: Option[EventLoggingListener] = _eventLogger

  private[spark] def executorAllocationManager: Option[ExecutorAllocationManager] =
    _executorAllocationManager

  private[spark] def cleaner: Option[ContextCleaner] = _cleaner

  private[spark] var checkpointDir: Option[String] = None

  // Thread Local variable that can be used by users to pass information down the stack
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = {
      // Note: make a clone such that changes in the parent properties aren't reflected in
      // the those of the children threads, which has confusing semantics (SPARK-10563).
      SerializationUtils.clone(parent)
    }
    override protected def initialValue(): Properties = new Properties()
  }

  /* ------------------------------------------------------------------------------------- *
   | Initialization. This code initializes the context in a manner that is exception-safe. |
   | All internal fields holding state are initialized here, and any error prompts the     |
   | stop() method to be called.                                                           |
   * ------------------------------------------------------------------------------------- */

  private def warnSparkMem(value: String): String = {
    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
      "deprecated, please use spark.executor.memory instead.")
    value
  }

  /** Control our logLevel. This overrides any user-defined log settings.
   * @param logLevel The desired log level as a string.
   * Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  def setLogLevel(logLevel: String) {
    // let's allow lowercase or mixed case too
    val upperCased = logLevel.toUpperCase(Locale.ROOT)
    require(SparkContext.VALID_LOG_LEVELS.contains(upperCased),
      s"Supplied level $logLevel did not match one of:" +
        s" ${SparkContext.VALID_LOG_LEVELS.mkString(",")}")
    Utils.setLogLevel(org.apache.log4j.Level.toLevel(upperCased))
  }

<<<<<<< HEAD
  def makePreCandidates(rdg: HashMap[Int, LineageInfo]): HashMap[Int, LineageInfo] = {
    rdg.filter(x => x._2.count > 1) //.filterNot(x => x._1 == 0)  // RDD0 and RDD1 are not always same
  }
  
  /*
  @volatile private var readRDGAlready = false
  private[spark] var readedRDG = HashMap[Int, LineageInfo]()
  private[spark] var preCandidates = HashMap[Int, LineageInfo]()

  // Really need this as a thread?, we need to run this only 1 time at the start
  def createPreCandidatesThread(): Thread = {
    new Thread(new Runnable {
      override def run(): Unit = {
        logInfoSSP("PreCandidates Thread start", isSSparkLogEnabled)
        readedRDG = readRDG(newAppName)
        readRDGAlready = true
        preCandidates = makePreCandidates(readedRDG)
        logInfoSSP(s"readRDG ${readedRDG}", isSSparkLogEnabled)
        logInfoSSP(s"preCandidates ${preCandidates}", isSSparkLogEnabled)
        logInfoSSP("PreCandidates Thread stop", isSSparkLogEnabled)
      }
    })
  }*/

  private[spark] val newAppName = creationSite.shortForm.split(" at ")(1).split('.')(0)
  private[spark] val readedRDG: HashMap[Int, LineageInfo] = readRDG()
  private[spark] val preCandidates: HashMap[Int, LineageInfo] = makePreCandidates(readedRDG)
  /*private[spark] var cacheCandidates: HashSet[String] = preCandidates
                                                        .map(x => s"${x._2.name}(${x._2.callSite})")
                                                        .toSet.to[HashSet]*/
  private[spark] val cacheCandidates = LinkedHashSet[CandidatesInfo]()
  /*
  preCandidates.foreach{x =>
    cacheCandidates.add(new CandidatesInfo(s"${x._2.name}(${x._2.callSite})"))
    cacheCandidates.last.counts = x._2.count
    cacheCandidates.last.addParents(preCandidates.filter(y => y._2.deps.contains(x._1))
      .map(x => s"${x._2.name}(${x._2.callSite})")
      .toSet
      .to[HashSet])
  }*/
  
  preCandidates.foreach {x =>
    val opId = s"${x._2.name}(${x._2.callSite})"
    val parents = preCandidates.filter(y => y._2.deps.contains(x._1))
=======
  val mutableConf = HashMap[String, Boolean]()
  
  def setOptimizeEnabled(value: Boolean): Unit = {
    mutableConf.put("optimize", value)
  }
  
  def setUnpersistPlan(value: Boolean): Unit = {
    mutableConf.put("unpersist", value)
  }

  def getMutableConf(key: String): Boolean = {
    mutableConf.get(key) match {
      case Some(x) => x
      case _ => false
    }
  }

  private[spark] val isSSparkPrepareEnabled  = config.getBoolean("spark.ss.prepare.enabled", false)
  private[spark] val isSSparkProfileEnabled  = config.getBoolean("spark.ss.profile.enabled", false)
  private[spark] val maxProfile              = config.getInt    ("spark.ss.profile.max", 1000)
  /*private[spark] val sizeProfileMode         = conf.getOption("spark.ss.profile.mode")
                                                   .getOrElse("toArray")  */
  //Optimize flag is can be changed at runtime
  private[spark] var isSSparkOptimizeEnabled = config.getBoolean("spark.ss.optimize.enabled", false)
  private[spark] val optimizeMode: String    = config.getOption ("spark.ss.optimize.mode").getOrElse("overlap")
  // optimizeMode: "overlap" or "ahead"
  private[spark] val isMature                = config.getBoolean("spark.ss.mature.enabled", false)

  private[spark] val cachePlan: String       = config.getOption ("spark.ss.cache.plan").getOrElse("refreshing")
  // cachePlan: "refreshing" or "enhancing" or "pruning"
  private[spark] var unpersistPlan           = config.getBoolean("spark.ss.unpersist.enabled", false)
  // unpersistPlan: "true" is on, "false" is off
	private[spark] val unpersistForceDisabled  = config.getBoolean("spark.ss.unpersist.forceDisabled", false)
  
  private[spark] val isSSparkMemCheck        = config.getBoolean("spark.ss.memCheck.enabled", false)
  private[spark] val isSSparkCountFunctional = config.getBoolean("spark.ss.countFunctional.enabled", false)
  private[spark] val isSSparkLogEnabled      = config.getBoolean("spark.ss.log.enabled", false)
  private[spark] val noCache                 = config.getBoolean("spark.ss.noCache.enabled", false) // experimental configuration
  
  private[spark] val profileThreshold        = config.getDouble("spark.ss.profile.threshold", 0.99)
  
  if (isSSparkOptimizeEnabled) setOptimizeEnabled(isSSparkOptimizeEnabled)
  if (unpersistPlan)           setUnpersistPlan(unpersistPlan)

  if ( noCache ) {
    isSSparkOptimizeEnabled = false
    setOptimizeEnabled(isSSparkOptimizeEnabled)
    unpersistPlan = false
    setUnpersistPlan(unpersistPlan)
  }

  def makePreCandidates(rdg: HashMap[Int, LineageInfo]): HashMap[Int, LineageInfo] = {
    rdg.filter(x => x._2.count > 1) //.filterNot(x => x._1 == 0)  // RDD0 and RDD1 are not always same
  }
  
  private[spark] val newAppName = creationSite.shortForm.split(" at ")(1).split('.')(0)
  private[spark] val readedRDG: HashMap[Int, LineageInfo] = readRDG()
  private[spark] val preCandidates: HashMap[Int, LineageInfo] = makePreCandidates(readedRDG)
  private[spark] val cacheCandidates = LinkedHashSet[CandidatesInfo]()
    
  preCandidates.foreach {x =>
    val opId = s"${x._2.name}(${x._2.callSite})"
    //val parents = preCandidates.filter(y => y._2.deps.contains(x._1))
    val parents = readedRDG.filter(y => y._2.deps.contains(x._1))
>>>>>>> LOSIC
      .map(x => s"${x._2.name}(${x._2.callSite})")
      .toSet
      .to[HashSet]
    if (!(parents.size == 1 && parents.last == opId)) {
      cacheCandidates.add(new CandidatesInfo(opId))
      cacheCandidates.last.counts = x._2.count
      cacheCandidates.last.addParents(parents)
    }
  }
  
<<<<<<< HEAD
  /*
  if (preCandidates.size != cacheCandidates.size) { // should be same? 
                                                    // No, because the RDD which has same OP as a parent will be eliminated
    logErrorSSP(s"Different size between preCandidates and cacheCandidates, need to check " +
      s"preCandidates: ${preCandidates.size} elements => $preCandidates" +
      s"cacheCandidates ${cacheCandidates.size} elements => $cacheCandidates")
  }*/
=======
  //private[spark] val originalCacheList = HashSet[Int]()
  //private[spark] val optimizeCacheList = HashSet[Int]()
>>>>>>> LOSIC

  private[spark] var cacheList = HashSet[String]()
  /*  cacheList example, Candidate should be name & callSite + column index not rddId
      cacheList elements will be cached.
      (Ex 1)
      HashSet[String]("objectFile(SVMWithSGDExample.scala:72:0)", 
                      "map(GeneralizedLinearAlgorithm.scala:297:0)", 
                      "randomSplit(SVMWithSGDExample.scala:75:1)") 
      (Ex 2)
      HashSet[String]("map(BinaryClassificationMetrics.scala:226:0)",
                      "map(SVMWithSGDExample.scala:86:1)", 
                      "objectFile(SVMWithSGDExample.scala:72:0)", 
                      "UnionRDD(BinaryClassificationMetrics.scala:90:0)", 
                      "mapPartitionsWithIndex(BinaryClassificationMetrics.scala:200:0)", 
                      "map(GeneralizedLinearAlgorithm.scala:297:0)", 
                      "map(SVMWithSGDExample.scala:86:0)", 
                      "combineByKey(BinaryClassificationMetrics.scala:151:0)", 
                      "map(BinaryClassificationMetrics.scala:209:0)", 
                      "randomSplit(SVMWithSGDExample.scala:75:1)", 
                      "sortByKey(BinaryClassificationMetrics.scala:155:0)", 
                      "randomSplit(SVMWithSGDExample.scala:75:0)")
  */

<<<<<<< HEAD
=======
  if ( cachePlan == "pruning" || cachePlan == "refreshing" ) {  // "refreshing" mode is experimental
    cacheList = cacheCandidates.map(_.opId).toSet.to[HashSet]
  }

>>>>>>> LOSIC
  def createSelectCandidatesThread(stageId: Int, rdd: RDD[_], stageInputBytes: Long): Thread = {
    new Thread(new Runnable {
      override def run(): Unit = {
        logInfoSSP("selectCandidates Thread start", isSSparkLogEnabled)
<<<<<<< HEAD
        /*if (!readRDGAlready) {
          readedRDG = readRDG(newAppName)
          readRDGAlready = true
          preCandidates = makePreCandidates(readedRDG)
          logInfoSSP(s"readRDG ${readedRDG}", isSSparkLogEnabled)
          logInfoSSP(s"preCandidates ${preCandidates}", isSSparkLogEnabled)
        }*/ //move to createPreCandidatesThread
        selectCandidates(stageId, stageInputBytes)
=======
        if (isMature) {
          if (stageId == 0)
            selectCandidates(stageInputBytes)
          selectCandidates(stageId)
        }
        else {
          selectCandidates(stageId, stageInputBytes)
        }
>>>>>>> LOSIC
        checkOptimizeCache(rdd)
        logInfoSSP("selectCandidates Thread stop", isSSparkLogEnabled)
      }
    })
  }

<<<<<<< HEAD
=======

>>>>>>> LOSIC
  try {
    _conf = config.clone()
    _conf.validateSettings()

    if (!_conf.contains("spark.master")) {
      throw new SparkException("A master URL must be set in your configuration")
    }
    if (!_conf.contains("spark.app.name")) {
      throw new SparkException("An application name must be set in your configuration")
    }

    // log out spark.app.name in the Spark driver logs
    logInfo(s"Submitted application: $appName")

    // System property spark.yarn.app.id must be set if user code ran by AM on a YARN cluster
    if (master == "yarn" && deployMode == "cluster" && !_conf.contains("spark.yarn.app.id")) {
      throw new SparkException("Detected yarn cluster mode, but isn't running on a cluster. " +
        "Deployment to YARN is not supported directly by SparkContext. Please use spark-submit.")
    }

    if (_conf.getBoolean("spark.logConf", false)) {
      logInfo("Spark configuration:\n" + _conf.toDebugString)
    }

    // Set Spark driver host and port system properties. This explicitly sets the configuration
    // instead of relying on the default value of the config constant.
    _conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_HOST_ADDRESS))
    _conf.setIfMissing("spark.driver.port", "0")

    _conf.set("spark.executor.id", SparkContext.DRIVER_IDENTIFIER)

    _jars = Utils.getUserJars(_conf)
    _files = _conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.nonEmpty))
      .toSeq.flatten

    _eventLogDir =
      if (isEventLogEnabled) {
        val unresolvedDir = conf.get("spark.eventLog.dir", EventLoggingListener.DEFAULT_LOG_DIR)
          .stripSuffix("/")
        Some(Utils.resolveURI(unresolvedDir))
      } else {
        None
      }

    _eventLogCodec = {
      val compress = _conf.getBoolean("spark.eventLog.compress", false)
      if (compress && isEventLogEnabled) {
        Some(CompressionCodec.getCodecName(_conf)).map(CompressionCodec.getShortName)
      } else {
        None
      }
    }

    _listenerBus = new LiveListenerBus(_conf)

    // Initialize the app status store and listener before SparkEnv is created so that it gets
    // all events.
    _statusStore = AppStatusStore.createLiveStore(conf)
    listenerBus.addToStatusQueue(_statusStore.listener.get)

    // Create the Spark execution environment (cache, map output tracker, etc)
    _env = createSparkEnv(_conf, isLocal, listenerBus)
    SparkEnv.set(_env)

    // If running the REPL, register the repl's output dir with the file server.
    _conf.getOption("spark.repl.class.outputDir").foreach { path =>
      val replUri = _env.rpcEnv.fileServer.addDirectory("/classes", new File(path))
      _conf.set("spark.repl.class.uri", replUri)
    }

    _statusTracker = new SparkStatusTracker(this, _statusStore)

    _progressBar =
      if (_conf.get(UI_SHOW_CONSOLE_PROGRESS) && !log.isInfoEnabled) {
        Some(new ConsoleProgressBar(this))
      } else {
        None
      }

    _ui =
      if (conf.getBoolean("spark.ui.enabled", true)) {
        Some(SparkUI.create(Some(this), _statusStore, _conf, _env.securityManager, appName, "",
          startTime))
      } else {
        // For tests, do not enable the UI
        None
      }
    // Bind the UI before starting the task scheduler to communicate
    // the bound port to the cluster manager properly
    _ui.foreach(_.bind())

    _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)

    // Add each JAR given through the constructor
    if (jars != null) {
      jars.foreach(addJar)
    }

    if (files != null) {
      files.foreach(addFile)
    }

    _executorMemory = _conf.getOption("spark.executor.memory")
      .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
      .orElse(Option(System.getenv("SPARK_MEM"))
      .map(warnSparkMem))
      .map(Utils.memoryStringToMb)
      .getOrElse(1024)

    // Convert java options to env vars as a work around
    // since we can't set env vars directly in sbt.
    for { (envKey, propKey) <- Seq(("SPARK_TESTING", "spark.testing"))
      value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
      executorEnvs(envKey) = value
    }
    Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
      executorEnvs("SPARK_PREPEND_CLASSES") = v
    }
    // The Mesos scheduler backend relies on this environment variable to set executor memory.
    // TODO: Set this only in the Mesos scheduler.
    executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
    executorEnvs ++= _conf.getExecutorEnv
    executorEnvs("SPARK_USER") = sparkUser

    // We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will
    // retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
    _heartbeatReceiver = env.rpcEnv.setupEndpoint(
      HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))

    // Create and start the scheduler
    val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
    _schedulerBackend = sched
    _taskScheduler = ts
    _dagScheduler = new DAGScheduler(this)
    _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)

    // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
    // constructor
    _taskScheduler.start()

    _applicationId = _taskScheduler.applicationId()
    _applicationAttemptId = taskScheduler.applicationAttemptId()
    _conf.set("spark.app.id", _applicationId)
    if (_conf.getBoolean("spark.ui.reverseProxy", false)) {
      System.setProperty("spark.ui.proxyBase", "/proxy/" + _applicationId)
    }
    _ui.foreach(_.setAppId(_applicationId))
    _env.blockManager.initialize(_applicationId)

    // The metrics system for Driver need to be set spark.app.id to app ID.
    // So it should start after we get app ID from the task scheduler and set spark.app.id.
    _env.metricsSystem.start()
    // Attach the driver metrics servlet handler to the web ui after the metrics system is started.
    _env.metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))

    _eventLogger =
      if (isEventLogEnabled) {
        val logger =
          new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,
            _conf, _hadoopConfiguration)
        logger.start()
        listenerBus.addToEventLogQueue(logger)
        Some(logger)
      } else {
        None
      }

    // Optionally scale number of executors dynamically based on workload. Exposed for testing.
    val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
    _executorAllocationManager =
      if (dynamicAllocationEnabled) {
        schedulerBackend match {
          case b: ExecutorAllocationClient =>
            Some(new ExecutorAllocationManager(
              schedulerBackend.asInstanceOf[ExecutorAllocationClient], listenerBus, _conf,
              _env.blockManager.master))
          case _ =>
            None
        }
      } else {
        None
      }
    _executorAllocationManager.foreach(_.start())

    _cleaner =
      if (_conf.getBoolean("spark.cleaner.referenceTracking", true)) {
        Some(new ContextCleaner(this))
      } else {
        None
      }
    _cleaner.foreach(_.start())

    setupAndStartListenerBus()
    postEnvironmentUpdate()
    postApplicationStart()

    // Post init
    _taskScheduler.postStartHook()
    _env.metricsSystem.registerSource(_dagScheduler.metricsSource)
    _env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
    _executorAllocationManager.foreach { e =>
      _env.metricsSystem.registerSource(e.executorAllocationManagerSource)
    }

    // Make sure the context is stopped if the user forgets about it. This avoids leaving
    // unfinished event logs around after the JVM exits cleanly. It doesn't help if the JVM
    // is killed, though.
    logDebug("Adding shutdown hook") // force eager creation of logger
    _shutdownHookRef = ShutdownHookManager.addShutdownHook(
      ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
      logInfo("Invoking stop() from shutdown hook")
      try {
        stop()
      } catch {
        case e: Throwable =>
          logWarning("Ignoring Exception while stopping SparkContext from shutdown hook", e)
      }
    }
  } catch {
    case NonFatal(e) =>
      logError("Error initializing SparkContext.", e)
      try {
        stop()
      } catch {
        case NonFatal(inner) =>
          logError("Error stopping SparkContext after init error.", inner)
      } finally {
        throw e
      }
  }

  /**
   * Called by the web UI to obtain executor thread dumps.  This method may be expensive.
   * Logs an error and returns None if we failed to obtain a thread dump, which could occur due
   * to an executor being dead or unresponsive or due to network issues while sending the thread
   * dump message back to the driver.
   */
  private[spark] def getExecutorThreadDump(executorId: String): Option[Array[ThreadStackTrace]] = {
    try {
      if (executorId == SparkContext.DRIVER_IDENTIFIER) {
        Some(Utils.getThreadDump())
      } else {
        val endpointRef = env.blockManager.master.getExecutorEndpointRef(executorId).get
        Some(endpointRef.askSync[Array[ThreadStackTrace]](TriggerThreadDump))
      }
    } catch {
      case e: Exception =>
        logError(s"Exception getting thread dump from executor $executorId", e)
        None
    }
  }

  private[spark] def getLocalProperties: Properties = localProperties.get()

  private[spark] def setLocalProperties(props: Properties) {
    localProperties.set(props)
  }

  /**
   * Set a local property that affects jobs submitted from this thread, such as the Spark fair
   * scheduler pool. User-defined properties may also be set here. These properties are propagated
   * through to worker tasks and can be accessed there via
   * [[org.apache.spark.TaskContext#getLocalProperty]].
   *
   * These properties are inherited by child threads spawned from this thread. This
   * may have unexpected consequences when working with thread pools. The standard java
   * implementation of thread pools have worker threads spawn other worker threads.
   * As a result, local properties may propagate unpredictably.
   */
  def setLocalProperty(key: String, value: String) {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  /**
   * Get a local property set in this thread, or null if it is missing. See
   * `org.apache.spark.SparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).orNull

  /** Set a human readable description of the current job. */
  def setJobDescription(value: String) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
  }

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
   *
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
   *
   * The application can also use `org.apache.spark.SparkContext.cancelJobGroup` to cancel all
   * running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   *
   * @param interruptOnCancel If true, then job cancellation will result in `Thread.interrupt()`
   * being called on the job's executor threads. This is useful to help ensure that the tasks
   * are actually stopped in a timely manner, but is off by default due to HDFS-1208, where HDFS
   * may respond to Thread.interrupt() by marking nodes as dead.
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean = false) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
    // Note: Specifying interruptOnCancel in setJobGroup (rather than cancelJobGroup) avoids
    // changing several public APIs and allows Spark cancellations outside of the cancelJobGroup
    // APIs to also take advantage of this property (e.g., internal job failures or canceling from
    // JobProgressTab UI) on a per-job basis.
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
  }

  /** Clear the current thread's job group ID and its description. */
  def clearJobGroup() {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
  }

  /**
   * Execute a block of code in a scope such that all new RDDs created in this body will
   * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
   *
   * @note Return statements are NOT allowed in the given body.
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  // Methods for creating RDDs

  /** Distribute a local Scala collection to form an RDD.
   *
   * @note Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
   * to parallelize and before the first action on the RDD, the resultant RDD will reflect the
   * modified collection. Pass a copy of the argument to avoid this.
   * @note avoid using `parallelize(Seq())` to create an empty `RDD`. Consider `emptyRDD` for an
   * RDD with no partitions, or `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
   * @param seq Scala collection to distribute
   * @param numSlices number of partitions to divide the collection into
   * @return RDD representing distributed collection
   */
  def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    assertNotStopped()
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

  /**
   * Creates a new RDD[Long] containing elements from `start` to `end`(exclusive), increased by
   * `step` every element.
   *
   * @note if we need to cache this RDD, we should make sure each partition does not exceed limit.
   *
   * @param start the start value.
   * @param end the end value.
   * @param step the incremental step
   * @param numSlices number of partitions to divide the collection into
   * @return RDD representing distributed range
   */
  def range(
      start: Long,
      end: Long,
      step: Long = 1,
      numSlices: Int = defaultParallelism): RDD[Long] = withScope {
    assertNotStopped()
    // when step is 0, range will run infinitely
    require(step != 0, "step cannot be 0")
    val numElements: BigInt = {
      val safeStart = BigInt(start)
      val safeEnd = BigInt(end)
      if ((safeEnd - safeStart) % step == 0 || (safeEnd > safeStart) != (step > 0)) {
        (safeEnd - safeStart) / step
      } else {
        // the remainder has the same sign with range, could add 1 more
        (safeEnd - safeStart) / step + 1
      }
    }
    parallelize(0 until numSlices, numSlices).mapPartitionsWithIndex { (i, _) =>
      val partitionStart = (i * numElements) / numSlices * step + start
      val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
      def getSafeMargin(bi: BigInt): Long =
        if (bi.isValidLong) {
          bi.toLong
        } else if (bi > 0) {
          Long.MaxValue
        } else {
          Long.MinValue
        }
      val safePartitionStart = getSafeMargin(partitionStart)
      val safePartitionEnd = getSafeMargin(partitionEnd)

      new Iterator[Long] {
        private[this] var number: Long = safePartitionStart
        private[this] var overflow: Boolean = false

        override def hasNext =
          if (!overflow) {
            if (step > 0) {
              number < safePartitionEnd
            } else {
              number > safePartitionEnd
            }
          } else false

        override def next() = {
          val ret = number
          number += step
          if (number < ret ^ step < 0) {
            // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
            // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
            // back, we are pretty sure that we have an overflow.
            overflow = true
          }
          ret
        }
      }
    }
  }

  /** Distribute a local Scala collection to form an RDD.
   *
   * This method is identical to `parallelize`.
   * @param seq Scala collection to distribute
   * @param numSlices number of partitions to divide the collection into
   * @return RDD representing distributed collection
   */
  def makeRDD[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    parallelize(seq, numSlices)
  }

  /**
   * Distribute a local Scala collection to form an RDD, with one or more
   * location preferences (hostnames of Spark nodes) for each object.
   * Create a new partition for each collection item.
   * @param seq list of tuples of data and location preferences (hostnames of Spark nodes)
   * @return RDD representing data partitioned according to location preferences
   */
  def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = withScope {
    assertNotStopped()
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap
    new ParallelCollectionRDD[T](this, seq.map(_._1), math.max(seq.size, 1), indexToPrefs)
  }

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   * @param path path to the text file on a supported file system
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of lines of the text file
   */
  def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
   *
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
   *
   * <p> then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred, large file is also allowable, but may cause bad performance.
   * @note On some filesystems, `.../path/&#42;` can be a more efficient way to read all files
   *       in a directory rather than `.../path/` or `.../path`
   * @note Partitioning is determined by data locality. This may result in too few partitions
   *       by default.
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the
   *             list of inputs.
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   * @return RDD representing tuples of file path and the corresponding file content
   */
  def wholeTextFiles(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[(String, String)] = withScope {
    assertNotStopped()
    val job = NewHadoopJob.getInstance(hadoopConfiguration)
    // Use setInputPaths so that wholeTextFiles aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new WholeTextFileRDD(
      this,
      classOf[WholeTextFileInputFormat],
      classOf[Text],
      classOf[Text],
      updateConf,
      minPartitions).map(record => (record._1.toString, record._2.toString)).setName(path)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset as PortableDataStream for each file
   * (useful for binary data)
   *
   * For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * `val rdd = sparkContext.binaryFiles("hdfs://a-hdfs-path")`,
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files may cause bad performance.
   * @note On some filesystems, `.../path/&#42;` can be a more efficient way to read all files
   *       in a directory rather than `.../path/` or `.../path`
   * @note Partitioning is determined by data locality. This may result in too few partitions
   *       by default.
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the
   *             list of inputs.
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   * @return RDD representing tuples of file path and corresponding file content
   */
  def binaryFiles(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[(String, PortableDataStream)] = withScope {
    assertNotStopped()
    val job = NewHadoopJob.getInstance(hadoopConfiguration)
    // Use setInputPaths so that binaryFiles aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new BinaryFileRDD(
      this,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      updateConf,
      minPartitions).setName(path)
  }

  /**
   * Load data from a flat binary file, assuming the length of each record is constant.
   *
   * @note We ensure that the byte array for each record in the resulting RDD
   * has the provided record length.
   *
   * @param path Directory to the input data files, the path can be comma separated paths as the
   *             list of inputs.
   * @param recordLength The length at which to split the records
   * @param conf Configuration for setting up the dataset.
   *
   * @return An RDD of data with values, represented as byte arrays
   */
  def binaryRecords(
      path: String,
      recordLength: Int,
      conf: Configuration = hadoopConfiguration): RDD[Array[Byte]] = withScope {
    assertNotStopped()
    conf.setInt(FixedLengthBinaryInputFormat.RECORD_LENGTH_PROPERTY, recordLength)
    val br = newAPIHadoopFile[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](path,
      classOf[FixedLengthBinaryInputFormat],
      classOf[LongWritable],
      classOf[BytesWritable],
      conf = conf)
    br.map { case (k, v) =>
      val bytes = v.copyBytes()
      assert(bytes.length == recordLength, "Byte array does not have correct length")
      bytes
    }
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and other
   * necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable),
   * using the older MapReduce API (`org.apache.hadoop.mapred`).
   *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param inputFormatClass storage format of the data to be read
   * @param keyClass `Class` of the key associated with the `inputFormatClass` parameter
   * @param valueClass `Class` of the value associated with the `inputFormatClass` parameter
   * @param minPartitions Minimum number of Hadoop Splits to generate.
   * @return RDD of tuples of key and corresponding value
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   */
  def hadoopRDD[K, V](
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(conf)

    // Add necessary security credentials to the JobConf before broadcasting it.
    SparkHadoopUtil.get.addCredentials(conf)
    new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param inputFormatClass storage format of the data to be read
   * @param keyClass `Class` of the key associated with the `inputFormatClass` parameter
   * @param valueClass `Class` of the value associated with the `inputFormatClass` parameter
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(hadoopConfiguration)

    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)

    inputBytes = hadoopFileSize(path)

    new HadoopRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

  var inputBytes = 0L
  var shuffleBytes = 0L
  // SSPARK: get HDFS file size
  def hadoopFileSize(path: String): Long = {
    val filePath = new Path(path)
    val fs = filePath.getFileSystem(hadoopConfiguration)

    var totalSize = 0L
    
    def calculateTotalSize(path: Path): Long = {
      val status = fs.getFileStatus(path)
      if (status.isFile) {
        status.getLen
      } else {
        fs.listStatus(path).map { fileStatus =>
          calculateTotalSize(fileStatus.getPath)
        }.sum
      }
    }

    try {
      if (fs.exists(filePath)) {
        totalSize = calculateTotalSize(filePath)
        logInfoSSP(s"Total size of all files in $path: $totalSize bytes", isSSparkLogEnabled)
      } else {
        logErrorSSP(s"Path $path does not exist.")
      }
    } catch {
      case e: Exception =>
        logErrorSSP(s"Error retrieving file size for $path: ${e.getMessage}")
    }
    totalSize
  }

  def getStageInputBytes(lineage: LinkedHashMap[Int, LineageInfo],
                                 rootLineage: Int): (Long, String) = {
    if (lineage.get(rootLineage).get.deps.contains(-2)) 
      (inputBytes, "HadoopRDD")
    else if (lineage.get(rootLineage).get.deps.contains(-1))
      (shuffleBytes, "ShuffleRDD")
    else if (lineage.get(rootLineage).get.deps.contains(-3)) 
      (0L, "ParallelCollectionRDD")
    else {
      logErrorSSP(s"Cannot find stage input size ${lineage.get(rootLineage)}")
      (0L, "ERROR")
    }
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
   * }}}
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]]
      (path: String, minPartitions: Int)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    hadoopFile(path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]],
      minPartitions)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * }}}
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths as
   * a list of inputs
   * @return RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    hadoopFile[K, V, F](path, defaultMinPartitions)
  }

  /**
   * Smarter version of `newApiHadoopFile` that uses class tags to figure out the classes of keys,
   * values and the `org.apache.hadoop.mapreduce.InputFormat` (new MapReduce API) so that user
   * don't need to pass them directly. Instead, callers can just write, for example:
   * ```
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * ```
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @return RDD of tuples of key and corresponding value
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
      (path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
<<<<<<< HEAD
=======
      
    inputBytes = hadoopFileSize(path)

>>>>>>> LOSIC
    newAPIHadoopFile(
      path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param fClass storage format of the data to be read
   * @param kClass `Class` of the key associated with the `fClass` parameter
   * @param vClass `Class` of the value associated with the `fClass` parameter
   * @param conf Hadoop configuration
   * @return RDD of tuples of key and corresponding value
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(hadoopConfiguration)

    // The call to NewHadoopJob automatically adds security credentials to conf,
    // so we don't need to explicitly add them ourselves
    val job = NewHadoopJob.getInstance(conf)
    // Use setInputPaths so that newAPIHadoopFile aligns with hadoopFile/textFile in taking
    // comma separated files as input. (see SPARK-7155)
    NewFileInputFormat.setInputPaths(job, path)
    val updatedConf = job.getConfiguration
    new NewHadoopRDD(this, fClass, kClass, vClass, updatedConf).setName(path)
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * @param conf Configuration for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param fClass storage format of the data to be read
   * @param kClass `Class` of the key associated with the `fClass` parameter
   * @param vClass `Class` of the value associated with the `fClass` parameter
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(conf)

    // Add necessary security credentials to the JobConf. Required to access secure HDFS.
    val jconf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jconf)
    new NewHadoopRDD(this, fClass, kClass, vClass, jconf)
  }

  /**
   * Get an RDD for a Hadoop SequenceFile with given key and value types.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param keyClass `Class` of the key associated with `SequenceFileInputFormat`
   * @param valueClass `Class` of the value associated with `SequenceFileInputFormat`
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  def sequenceFile[K, V](path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int
      ): RDD[(K, V)] = withScope {
    assertNotStopped()
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /**
   * Get an RDD for a Hadoop SequenceFile with given key and value types.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param keyClass `Class` of the key associated with `SequenceFileInputFormat`
   * @param valueClass `Class` of the value associated with `SequenceFileInputFormat`
   * @return RDD of tuples of key and corresponding value
   */
  def sequenceFile[K, V](
      path: String,
      keyClass: Class[K],
      valueClass: Class[V]): RDD[(K, V)] = withScope {
    assertNotStopped()
    sequenceFile(path, keyClass, valueClass, defaultMinPartitions)
  }

  /**
   * Version of sequenceFile() for types implicitly convertible to Writables through a
   * WritableConverter. For example, to access a SequenceFile where the keys are Text and the
   * values are IntWritable, you could simply write
   * {{{
   * sparkContext.sequenceFile[String, Int](path, ...)
   * }}}
   *
   * WritableConverters are provided in a somewhat strange way (by an implicit function) to support
   * both subclasses of Writable and types for which we define a converter (e.g. Int to
   * IntWritable). The most natural thing would've been to have implicit objects for the
   * converters, but then we couldn't have an object for every subclass of Writable (you can't
   * have a parameterized singleton object). We use functions instead to create a new converter
   * for the appropriate type. In addition, we pass the converter a ClassTag of its type to
   * allow it to figure out the Writable class to use in the subclass case.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
   def sequenceFile[K, V]
       (path: String, minPartitions: Int = defaultMinPartitions)
       (implicit km: ClassTag[K], vm: ClassTag[V],
        kcf: () => WritableConverter[K], vcf: () => WritableConverter[V]): RDD[(K, V)] = {
    withScope {
      assertNotStopped()
      val kc = clean(kcf)()
      val vc = clean(vcf)()
      val format = classOf[SequenceFileInputFormat[Writable, Writable]]
      val writables = hadoopFile(path, format,
        kc.writableClass(km).asInstanceOf[Class[Writable]],
        vc.writableClass(vm).asInstanceOf[Class[Writable]], minPartitions)
      writables.map { case (k, v) => (kc.convert(k), vc.convert(v)) }
    }
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental
   * storage format and may not be supported exactly as is in future Spark releases. It will also
   * be pretty slow if you use the default serializer (Java serialization),
   * though the nice thing about it is that there's very little effort required to save arbitrary
   * objects.
   *
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD representing deserialized data from the file(s)
   */
  def objectFile[T: ClassTag](
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[T] = withScope {
    assertNotStopped()
    sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes, Utils.getContextOrSparkClassLoader))
  }

  protected[spark] def checkpointFile[T: ClassTag](path: String): RDD[T] = withScope {
    new ReliableCheckpointRDD[T](this, path)
  }

  /** Build the union of a list of RDDs. */
  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = withScope {
    val nonEmptyRdds = rdds.filter(!_.partitions.isEmpty)
    val partitioners = nonEmptyRdds.flatMap(_.partitioner).toSet
    if (nonEmptyRdds.forall(_.partitioner.isDefined) && partitioners.size == 1) {
      new PartitionerAwareUnionRDD(this, nonEmptyRdds)
    } else {
      new UnionRDD(this, nonEmptyRdds)
    }
  }

  /** Build the union of a list of RDDs passed as variable-length arguments. */
  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] = withScope {
    union(Seq(first) ++ rest)
  }

  /** Get an RDD that has no partitions or elements. */
  def emptyRDD[T: ClassTag]: RDD[T] = new EmptyRDD[T](this)

  // Methods for creating shared variables

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
   * values to using the `+=` method. Only the driver can access the accumulator's `value`.
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]): Accumulator[T] = {
    val acc = new Accumulator(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, with a name for display
   * in the Spark UI. Tasks can "add" values to the accumulator using the `+=` method. Only the
   * driver can access the accumulator's `value`.
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T])
    : Accumulator[T] = {
    val acc = new Accumulator(initialValue, param, Option(name))
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, to which tasks can add values
   * with `+=`. Only the driver can access the accumulable's `value`.
   * @tparam R accumulator result type
   * @tparam T type that can be added to the accumulator
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulable[R, T](initialValue: R)(implicit param: AccumulableParam[R, T])
    : Accumulable[R, T] = {
    val acc = new Accumulable(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, with a name for display in the
   * Spark UI. Tasks can add values to the accumulable using the `+=` operator. Only the driver can
   * access the accumulable's `value`.
   * @tparam R accumulator result type
   * @tparam T type that can be added to the accumulator
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulable[R, T](initialValue: R, name: String)(implicit param: AccumulableParam[R, T])
    : Accumulable[R, T] = {
    val acc = new Accumulable(initialValue, param, Option(name))
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Create an accumulator from a "mutable collection" type.
   *
   * Growable and TraversableOnce are the standard APIs that guarantee += and ++=, implemented by
   * standard mutable collections. So you can use this with mutable Map, Set, etc.
   */
  @deprecated("use AccumulatorV2", "2.0.0")
  def accumulableCollection[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
      (initialValue: R): Accumulable[R, T] = {
    // TODO the context bound (<%) above should be replaced with simple type bound and implicit
    // conversion but is a breaking change. This should be fixed in Spark 3.x.
    val param = new GrowableAccumulableParam[R, T]
    val acc = new Accumulable(initialValue, param)
    cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
    acc
  }

  /**
   * Register the given accumulator.
   *
   * @note Accumulators must be registered before use, or it will throw exception.
   */
  def register(acc: AccumulatorV2[_, _]): Unit = {
    acc.register(this)
  }

  /**
   * Register the given accumulator with given name.
   *
   * @note Accumulators must be registered before use, or it will throw exception.
   */
  def register(acc: AccumulatorV2[_, _], name: String): Unit = {
    acc.register(this, name = Option(name))
  }

  /**
   * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
   */
  def longAccumulator: LongAccumulator = {
    val acc = new LongAccumulator
    register(acc)
    acc
  }

  /**
   * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
   */
  def longAccumulator(name: String): LongAccumulator = {
    val acc = new LongAccumulator
    register(acc, name)
    acc
  }

  /**
   * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
   */
  def doubleAccumulator: DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc)
    acc
  }

  /**
   * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
   */
  def doubleAccumulator(name: String): DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc, name)
    acc
  }

  /**
   * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
   * inputs by adding them into the list.
   */
  def collectionAccumulator[T]: CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc)
    acc
  }

  /**
   * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
   * inputs by adding them into the list.
   */
  def collectionAccumulator[T](name: String): CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc, name)
    acc
  }

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * The variable will be sent to each cluster only once.
   *
   * @param value value to broadcast to the Spark nodes
   * @return `Broadcast` object, a read-only variable cached on each machine
   */
  def broadcast[T: ClassTag](value: T): Broadcast[T] = {
    assertNotStopped()
    require(!classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass),
      "Can not directly broadcast RDDs; instead, call collect() and broadcast the result.")
    val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
    val callSite = getCallSite
    logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
    cleaner.foreach(_.registerBroadcastForCleanup(bc))
    bc
  }

  /**
   * Add a file to be downloaded with this Spark job on every node.
   *
   * If a file is added during execution, it will not be available until the next TaskSet starts.
   *
   * @param path can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
   *
   * @note A path can be added only once. Subsequent additions of the same path are ignored.
   */
  def addFile(path: String): Unit = {
    addFile(path, false)
  }

  /**
   * Returns a list of file paths that are added to resources.
   */
  def listFiles(): Seq[String] = addedFiles.keySet.toSeq

  /**
   * Add a file to be downloaded with this Spark job on every node.
   *
   * If a file is added during execution, it will not be available until the next TaskSet starts.
   *
   * @param path can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
   * @param recursive if true, a directory can be given in `path`. Currently directories are
   * only supported for Hadoop-supported filesystems.
   *
   * @note A path can be added only once. Subsequent additions of the same path are ignored.
   */
  def addFile(path: String, recursive: Boolean): Unit = {
    val uri = new Path(path).toUri
    val schemeCorrectedPath = uri.getScheme match {
      case null => new File(path).getCanonicalFile.toURI.toString
      case "local" =>
        logWarning("File with 'local' scheme is not supported to add to file server, since " +
          "it is already available on every node.")
        return
      case _ => path
    }

    val hadoopPath = new Path(schemeCorrectedPath)
    val scheme = new URI(schemeCorrectedPath).getScheme
    if (!Array("http", "https", "ftp").contains(scheme)) {
      val fs = hadoopPath.getFileSystem(hadoopConfiguration)
      val isDir = fs.getFileStatus(hadoopPath).isDirectory
      if (!isLocal && scheme == "file" && isDir) {
        throw new SparkException(s"addFile does not support local directories when not running " +
          "local mode.")
      }
      if (!recursive && isDir) {
        throw new SparkException(s"Added file $hadoopPath is a directory and recursive is not " +
          "turned on.")
      }
    } else {
      // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
      Utils.validateURL(uri)
    }

    val key = if (!isLocal && scheme == "file") {
      env.rpcEnv.fileServer.addFile(new File(uri.getPath))
    } else {
      schemeCorrectedPath
    }
    val timestamp = System.currentTimeMillis
    if (addedFiles.putIfAbsent(key, timestamp).isEmpty) {
      logInfo(s"Added file $path at $key with timestamp $timestamp")
      // Fetch the file locally so that closures which are run on the driver can still use the
      // SparkFiles API to access files.
      Utils.fetchFile(uri.toString, new File(SparkFiles.getRootDirectory()), conf,
        env.securityManager, hadoopConfiguration, timestamp, useCache = false)
      postEnvironmentUpdate()
    } else {
      logWarning(s"The path $path has been added already. Overwriting of added paths " +
       "is not supported in the current version.")
    }
  }

  /**
   * :: DeveloperApi ::
   * Register a listener to receive up-calls from events that happen during execution.
   */
  @DeveloperApi
  def addSparkListener(listener: SparkListenerInterface) {
    listenerBus.addToSharedQueue(listener)
  }

  /**
   * :: DeveloperApi ::
   * Deregister the listener from Spark's listener bus.
   */
  @DeveloperApi
  def removeSparkListener(listener: SparkListenerInterface): Unit = {
    listenerBus.removeListener(listener)
  }

  private[spark] def getExecutorIds(): Seq[String] = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        b.getExecutorIds()
      case _ =>
        logWarning("Requesting executors is not supported by current scheduler.")
        Nil
    }
  }

  /**
   * Get the max number of tasks that can be concurrent launched currently.
   * Note that please don't cache the value returned by this method, because the number can change
   * due to add/remove executors.
   *
   * @return The max number of tasks that can be concurrent launched currently.
   */
  private[spark] def maxNumConcurrentTasks(): Int = schedulerBackend.maxNumConcurrentTasks()

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  @DeveloperApi
  def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: scala.collection.immutable.Map[String, Int]
    ): Boolean = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        b.requestTotalExecutors(numExecutors, localityAwareTasks, hostToLocalTaskCount)
      case _ =>
        logWarning("Requesting executors is not supported by current scheduler.")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is received.
   */
  @DeveloperApi
  def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        b.requestExecutors(numAdditionalExecutors)
      case _ =>
        logWarning("Requesting executors is not supported by current scheduler.")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request that the cluster manager kill the specified executors.
   *
   * This is not supported when dynamic allocation is turned on.
   *
   * @note This is an indication to the cluster manager that the application wishes to adjust
   * its resource usage downwards. If the application wishes to replace the executors it kills
   * through this method with new ones, it should follow up explicitly with a call to
   * {{SparkContext#requestExecutors}}.
   *
   * @return whether the request is received.
   */
  @DeveloperApi
  def killExecutors(executorIds: Seq[String]): Boolean = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        require(executorAllocationManager.isEmpty,
          "killExecutors() unsupported with Dynamic Allocation turned on")
        b.killExecutors(executorIds, adjustTargetNumExecutors = true, countFailures = false,
          force = true).nonEmpty
      case _ =>
        logWarning("Killing executors is not supported by current scheduler.")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request that the cluster manager kill the specified executor.
   *
   * @note This is an indication to the cluster manager that the application wishes to adjust
   * its resource usage downwards. If the application wishes to replace the executor it kills
   * through this method with a new one, it should follow up explicitly with a call to
   * {{SparkContext#requestExecutors}}.
   *
   * @return whether the request is received.
   */
  @DeveloperApi
  def killExecutor(executorId: String): Boolean = killExecutors(Seq(executorId))

  /**
   * Request that the cluster manager kill the specified executor without adjusting the
   * application resource requirements.
   *
   * The effect is that a new executor will be launched in place of the one killed by
   * this request. This assumes the cluster manager will automatically and eventually
   * fulfill all missing application resource requests.
   *
   * @note The replace is by no means guaranteed; another application on the same cluster
   * can steal the window of opportunity and acquire this application's resources in the
   * mean time.
   *
   * @return whether the request is received.
   */
  private[spark] def killAndReplaceExecutor(executorId: String): Boolean = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        b.killExecutors(Seq(executorId), adjustTargetNumExecutors = false, countFailures = true,
          force = true).nonEmpty
      case _ =>
        logWarning("Killing executors is not supported by current scheduler.")
        false
    }
  }

  /** The version of Spark on which this application is running. */
  def version: String = SPARK_VERSION

  /**
   * Return a map from the slave to the max memory available for caching and the remaining
   * memory available for caching.
   */
  def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
    assertNotStopped()
    env.blockManager.master.getMemoryStatus.map { case(blockManagerId, mem) =>
      (blockManagerId.host + ":" + blockManagerId.port, mem)
    }
  }

  /**
   * :: DeveloperApi ::
   * Return information about what RDDs are cached, if they are in mem or on disk, how much space
   * they take, etc.
   */
  @DeveloperApi
  def getRDDStorageInfo: Array[RDDInfo] = {
    getRDDStorageInfo(_ => true)
  }

  private[spark] def getRDDStorageInfo(filter: RDD[_] => Boolean): Array[RDDInfo] = {
    assertNotStopped()
    val rddInfos = persistentRdds.values.filter(filter).map(RDDInfo.fromRdd).toArray
    rddInfos.foreach { rddInfo =>
      val rddId = rddInfo.id
      val rddStorageInfo = statusStore.asOption(statusStore.rdd(rddId))
      rddInfo.numCachedPartitions = rddStorageInfo.map(_.numCachedPartitions).getOrElse(0)
      rddInfo.memSize = rddStorageInfo.map(_.memoryUsed).getOrElse(0L)
      rddInfo.diskSize = rddStorageInfo.map(_.diskUsed).getOrElse(0L)
    }
    rddInfos.filter(_.isCached)
  }

  /**
   * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
   *
   * @note This does not necessarily mean the caching or computation was successful.
   */
  def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

  /**
   * :: DeveloperApi ::
   * Return pools for fair scheduler
   */
  @DeveloperApi
  def getAllPools: Seq[Schedulable] = {
    assertNotStopped()
    // TODO(xiajunluan): We should take nested pools into account
    taskScheduler.rootPool.schedulableQueue.asScala.toSeq
  }

  /**
   * :: DeveloperApi ::
   * Return the pool associated with the given name, if one exists
   */
  @DeveloperApi
  def getPoolForName(pool: String): Option[Schedulable] = {
    assertNotStopped()
    Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
  }

  /**
   * Return current scheduling mode
   */
  def getSchedulingMode: SchedulingMode.SchedulingMode = {
    assertNotStopped()
    taskScheduler.schedulingMode
  }

  /**
   * Gets the locality information associated with the partition in a particular rdd
   * @param rdd of interest
   * @param partition to be looked up for locality
   * @return list of preferred locations for the partition
   */
  private [spark] def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    dagScheduler.getPreferredLocs(rdd, partition)
  }

  /**
   * Register an RDD to be persisted in memory and/or disk storage
   */
  private[spark] def persistRDD(rdd: RDD[_]) {
    persistentRdds(rdd.id) = rdd
  }

  /**
   * Unpersist an RDD from memory and/or disk storage
   */
  private[spark] def unpersistRDD(rddId: Int, blocking: Boolean = true) {
    env.blockManager.master.removeRdd(rddId, blocking)
    persistentRdds.remove(rddId)
    listenerBus.post(SparkListenerUnpersistRDD(rddId))
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this `SparkContext` in the future.
   *
   * If a jar is added during execution, it will not be available until the next TaskSet starts.
   *
   * @param path can be either a local file, a file in HDFS (or other Hadoop-supported filesystems),
   * an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
   *
   * @note A path can be added only once. Subsequent additions of the same path are ignored.
   */
  def addJar(path: String) {
    def addJarFile(file: File): String = {
      try {
        if (!file.exists()) {
          throw new FileNotFoundException(s"Jar ${file.getAbsolutePath} not found")
        }
        if (file.isDirectory) {
          throw new IllegalArgumentException(
            s"Directory ${file.getAbsoluteFile} is not allowed for addJar")
        }
        env.rpcEnv.fileServer.addJar(file)
      } catch {
        case NonFatal(e) =>
          logError(s"Failed to add $path to Spark environment", e)
          null
      }
    }

    if (path == null) {
      logWarning("null specified as parameter to addJar")
    } else {
      val key = if (path.contains("\\")) {
        // For local paths with backslashes on Windows, URI throws an exception
        addJarFile(new File(path))
      } else {
        val uri = new URI(path)
        // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
        Utils.validateURL(uri)
        uri.getScheme match {
          // A JAR file which exists only on the driver node
          case null =>
            // SPARK-22585 path without schema is not url encoded
            addJarFile(new File(uri.getRawPath))
          // A JAR file which exists only on the driver node
          case "file" => addJarFile(new File(uri.getPath))
          // A JAR file which exists locally on every worker node
          case "local" => "file:" + uri.getPath
          case _ => path
        }
      }
      if (key != null) {
        val timestamp = System.currentTimeMillis
        if (addedJars.putIfAbsent(key, timestamp).isEmpty) {
          logInfo(s"Added JAR $path at $key with timestamp $timestamp")
          postEnvironmentUpdate()
        } else {
          logWarning(s"The jar $path has been added already. Overwriting of added jars " +
            "is not supported in the current version.")
        }
      }
    }
  }

  /**
   * Returns a list of jar files that are added to resources.
   */
  def listJars(): Seq[String] = addedJars.keySet.toSeq

  /**
   * When stopping SparkContext inside Spark components, it's easy to cause dead-lock since Spark
   * may wait for some internal threads to finish. It's better to use this method to stop
   * SparkContext instead.
   */
  private[spark] def stopInNewThread(): Unit = {
    new Thread("stop-spark-context") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          SparkContext.this.stop()
        } catch {
          case e: Throwable =>
            logError(e.getMessage, e)
            throw e
        }
      }
    }.start()
  }

  /**
   * Shut down the SparkContext.
   */
  def stop(): Unit = {
<<<<<<< HEAD
=======
    // These logs are set to WARN due to the ALS log level.
    if (isSSparkMemCheck) {
      logWarningSSP(s"remainingMem ${getRemainingStorageMem()}, minMem $minRemainingStorageMem")
    }

>>>>>>> LOSIC
    if (LiveListenerBus.withinListenerThread.value) {
      throw new SparkException(s"Cannot stop SparkContext within listener bus thread.")
    }
    // Use the stopping variable to ensure no contention for the stop scenario.
    // Still track the stopped variable for use elsewhere in the code.
    if (!stopped.compareAndSet(false, true)) {
      logInfo("SparkContext already stopped.")
      return
    }
    if (_shutdownHookRef != null) {
      ShutdownHookManager.removeShutdownHook(_shutdownHookRef)
    }

    Utils.tryLogNonFatalError {
      postApplicationEnd()
    }
    Utils.tryLogNonFatalError {
      _ui.foreach(_.stop())
    }
    if (env != null) {
      Utils.tryLogNonFatalError {
        env.metricsSystem.report()
      }
    }
    Utils.tryLogNonFatalError {
      _cleaner.foreach(_.stop())
    }
    Utils.tryLogNonFatalError {
      _executorAllocationManager.foreach(_.stop())
    }
    if (_dagScheduler != null) {
      Utils.tryLogNonFatalError {
        _dagScheduler.stop()
      }
      _dagScheduler = null
    }
    if (_listenerBusStarted) {
      Utils.tryLogNonFatalError {
        listenerBus.stop()
        _listenerBusStarted = false
      }
    }
    Utils.tryLogNonFatalError {
      _eventLogger.foreach(_.stop())
    }
    if (env != null && _heartbeatReceiver != null) {
      Utils.tryLogNonFatalError {
        env.rpcEnv.stop(_heartbeatReceiver)
      }
    }
    Utils.tryLogNonFatalError {
      _progressBar.foreach(_.stop())
    }
    _taskScheduler = null
    // TODO: Cache.stop()?
    if (_env != null) {
      Utils.tryLogNonFatalError {
        _env.stop()
      }
      SparkEnv.set(null)
    }
    if (_statusStore != null) {
      _statusStore.close()
    }
<<<<<<< HEAD
    if (isSSparkProfileEnabled){
      val rdg = makeRDG(lineages)
      writeRDG(rdg)
=======
        
    if (isSSparkProfileEnabled){
      val profiledRatio = (computedCount - profiledCount).toDouble / computedCount
      // change log level to Warn because of ALS.
      logWarningSSP(s"Skip Profiling Ratio = $profiledRatio, " +
          s"$profiledCount rdds have been profiled of $computedCount computed rdds")
      if (isSSparkCountFunctional) {
        val functionalRatio = countFunctional.toDouble / computedCount
        logWarningSSP(s"Functional Ratio = $functionalRatio, " +
          s"$countFunctional functional rdds have been computed of $computedCount computed rdds")
      }
      val rdg = makeRDG(lineages)
      writeRDG(rdg)
      profilingTrim(profilingDir)
>>>>>>> LOSIC
    }
    // Clear this `InheritableThreadLocal`, or it will still be inherited in child threads even this
    // `SparkContext` is stopped.
    localProperties.remove()
    // Unset YARN mode system env variable, to allow switching between cluster types.
    SparkContext.clearActiveContext()
    logInfo("Successfully stopped SparkContext")
<<<<<<< HEAD
    profilingTrim(profilingDir)
=======
    if (isSSparkPrepareEnabled) {
      //logInfoSSP(s"${rddSize}")
      logWarningSSP(s"Total RDD size = ${rddSize.values.sum}")
      logWarningSSP(s"Total reuse-RDD size = ${reuseRddSize.values.sum}")
    }

>>>>>>> LOSIC
  }


  /**
   * Get Spark's home location from either a value set through the constructor,
   * or the spark.home Java property, or the SPARK_HOME environment variable
   * (in that order of preference). If neither of these is set, return None.
   */
  private[spark] def getSparkHome(): Option[String] = {
    conf.getOption("spark.home").orElse(Option(System.getenv("SPARK_HOME")))
  }

  /**
   * Set the thread-local property for overriding the call sites
   * of actions and RDDs.
   */
  def setCallSite(shortCallSite: String) {
    setLocalProperty(CallSite.SHORT_FORM, shortCallSite)
  }

  /**
   * Set the thread-local property for overriding the call sites
   * of actions and RDDs.
   */
  private[spark] def setCallSite(callSite: CallSite) {
    setLocalProperty(CallSite.SHORT_FORM, callSite.shortForm)
    setLocalProperty(CallSite.LONG_FORM, callSite.longForm)
  }

  /**
   * Clear the thread-local property for overriding the call sites
   * of actions and RDDs.
   */
  def clearCallSite() {
    setLocalProperty(CallSite.SHORT_FORM, null)
    setLocalProperty(CallSite.LONG_FORM, null)
  }

  /**
   * Capture the current user callsite and return a formatted version for printing. If the user
   * has overridden the call site using `setCallSite()`, this will return the user's version.
   */
  private[spark] def getCallSite(): CallSite = {
    lazy val callSite = Utils.getCallSite()
    CallSite(
      Option(getLocalProperty(CallSite.SHORT_FORM)).getOrElse(callSite.shortForm),
      Option(getLocalProperty(CallSite.LONG_FORM)).getOrElse(callSite.longForm)
    )
  }

<<<<<<< HEAD
  private[spark] val isSSparkLogEnabled = conf.getBoolean("spark.ssparkLog.enabled", false)
  private[spark] var isSSparkOptimizeEnabled = conf.getBoolean("spark.ssparkOptimize.enabled", false)
  //Optimize flag is can be changed at runtime
  private[spark] val isSSparkProfileEnabled = conf.getBoolean("spark.ssparkProfile.enabled", false)
  private[spark] val optimizeMode: String = conf.getOption("spark.sspark.optimizeMode").getOrElse("overlap")
  // optimizeMode: "overlap" or "ahead"
  private[spark] val cachePlan: String = conf.getOption("spark.sspark.cachePlan").getOrElse("refreshing")
  // cachePlan: "refreshing" or "enhancing" or "pruning"
  private[spark] val unpersistPlan = conf.getBoolean("spark.sspark.unpersistPlan", false)
  // unpersistPlan: "true" is on, "false" is off
  private[spark] val maxProfiling = conf.getInt("spark.sspark.maxProfiling", 1000)

  if (cachePlan == "pruning" || cachePlan == "refreshing") {  // not sure for "refreshing" mode
    cacheList = cacheCandidates.map(_.opId).toSet.to[HashSet]
  }

  logInfoSSP(s"== S-Spark: Steady-optimizing Spark configurations == \n" +
    s"\t\t\t\t\t\t ProfileEnabled  = $isSSparkProfileEnabled \t (true or FALSE)\n" +
    s"\t\t\t\t\t\t OptimizeEnabled = $isSSparkOptimizeEnabled \t (true or FALSE)\n" +
    s"\t\t\t\t\t\t OptimizeMode    = $optimizeMode \t (OVERLAP or ahead)\n" +
    s"\t\t\t\t\t\t CachePlan       = $cachePlan \t (REFRESHING or enhancing or pruning)\n" +
    s"\t\t\t\t\t\t UnpersistPlan   = $unpersistPlan \t (true or FALSE)\n" +
    s"\t\t\t\t\t\t LogEnabled      = $isSSparkLogEnabled \t (true or FALSE)\n" +
    s"\t\t\t\t\t\t maxProfiling    = $maxProfiling \t (default: 1000)\n" +
    s"\t\t\t\t\t\t #candidates     = ${cacheList.size}")
=======
  /*
  def setOptimizeEnabled(value: Boolean) {
    setLocalProperty("ss.optimize.enabled", value.toString)
  }

  def setUnpersistPlan(value: Boolean) {
    setLocalProperty("ss.unpersist.plan", value.toString)
  }

  def getOptimizeEnabled: Boolean = {
    getLocalProperty("ss.optimize.enabled") == "true"
  }

  def getUnpersistPlan: Boolean = {
    getLocalProperty("ss.unpersist.plan") == "true"
  }
  */

  logInfoSSP(s"== S-Spark: Steady-optimizing Spark configurations == \n" +
    s"\t\t\t\t\t\t ProfileEnabled  = $isSSparkProfileEnabled\t\t\t(true or FALSE)\n" +
    s"\t\t\t\t\t\t ProfileMax      = $maxProfile\t\t\t(default: 1000)\n" +
    s"\t\t\t\t\t\t OptimizeEnabled = $isSSparkOptimizeEnabled\t\t\t(true or FALSE)\n" +
    s"\t\t\t\t\t\t OptimizeMode    = $optimizeMode\t\t(OVERLAP or ahead)\n" +
    s"\t\t\t\t\t\t MatureEnabled   = $isMature\t\t\t(true or FALSE)\n" +
    s"\t\t\t\t\t\t CachePlan       = $cachePlan\t(REFRESHING or enhancing or pruning)\n" +
    s"\t\t\t\t\t\t UnpersistPlan   = $unpersistPlan\t\t\t(true or FALSE) \n" +
    s"\t\t\t\t\t\t LogEnabled      = $isSSparkLogEnabled\t\t\t(true or FALSE)\n" +
    s"\t\t\t\t\t\t #candidates     = ${cacheCandidates.size},  #cacheList = ${cacheList.size}")
>>>>>>> LOSIC

  // SSPARK: We classify rdd transformation operation into three types.
  val predictableOps = Array("objectFile", 
                             "sequenceFile", 
                             "textFile", 
                             "distinct", 
                             "partitionBy", 
                             "groupByKey", 
                             "join")
  val constantOps = Array("sortByKey", 
                          "zip", 
                          "zipPartitions", 
                          "zipWithIndex", 
                          "sample", 
                          "randomSplit")
  val functionalOps = Array("map", 
                            "mapPartitions", 
                            "mapPartitionsWithIndex", 
                            "mapValues", 
                            "flatMap", 
                            "filter")
  /* val shuffleOps = Array("treeAggregate", "aggregate", ...)
    We don't need to distinguish shuffle operations, just assume that unrecognized operations as a shuffle 
    or not-to be optimized
  */
<<<<<<< HEAD
=======

  val rddSize = HashMap[Int, Double]()
  val countMap = HashMap[Int, Int]()
  val reuseRddSize = HashMap[Int, Double]()
>>>>>>> LOSIC
  
  val accumScope = HashMap[Int, String]()
  def newRddCallSite[T](scope: T): String = { // must handle None case
    val scopeId = scope match {
      case None => -1
      case x: Option[RDDOperationScope] => x.get.id.toInt
    }
    
    val tmpStr = getCallSite.shortForm.split(" at ")
    var callSite = ""
    if (tmpStr.length == 2) {
      callSite = tmpStr(0).filterNot(_.isWhitespace) + "(" + tmpStr(1).filterNot(_.isWhitespace)
    }
    else
      logErrorSSP(s"Cannot get correct callSite ${tmpStr}")

    var newCallSite = ""
    if (accumScope.contains(scopeId)) {
      val newColumn = {
        if (callSite.startsWith("randomSplit")) 1
        else 0
      }
      newCallSite = callSite + ":" + newColumn.toString + ")"
      accumScope.put(scopeId, newCallSite)
    }
    else { // not same scopeId
      val newColumn = {
        val getSameLine = accumScope.find(x => x._2.startsWith(callSite))
        if (getSameLine != None) {
          getSameLine.maxBy(x => x._1)._2.split(":")(2).replace(")", "").toInt + 1
        }
        else 0
      }
      newCallSite = callSite + ":" + newColumn.toString + ")"
      accumScope.put(scopeId, newCallSite)
    }
    newCallSite
  }
<<<<<<< HEAD
  
  @deprecated
  def newRddCallSite_depr(scopeId: Int): String = {
    val tmpStr = getCallSite.shortForm.split(" at ")
    var callSite = ""
    if (tmpStr.length == 2) {
      callSite = tmpStr(0).filterNot(_.isWhitespace) + "(" + tmpStr(1).filterNot(_.isWhitespace)
    }
    else
      logErrorSSP(s"Cannot get correct callSite ${tmpStr}")

    var newCallSite = ""
    if (accumScope.contains(scopeId)) {
      val newColumn = {
        if (callSite.startsWith("randomSplit")) 1
        else 0
      }
      newCallSite = callSite + ":" + newColumn.toString + ")"
      accumScope.put(scopeId, newCallSite)
    }
    else { // not same scopeId
      val newColumn = {
        val getSameLine = accumScope.find(x => x._2.startsWith(callSite))
        if (getSameLine != None) {
          getSameLine.maxBy(x => x._1)._2.split(":")(2).replace(")", "").toInt + 1
        }
        else 0
      }
      newCallSite = callSite + ":" + newColumn.toString + ")"
      accumScope.put(scopeId, newCallSite)
    }
    newCallSite
  }
  
=======
    
>>>>>>> LOSIC
  def isShuffleOp(op: String): Boolean = {
    !(predictableOps.contains(op) | constantOps.contains(op) | functionalOps.contains(op))
  }
  
  class LineageInfo(val _name: String,
                    val _callSite: String) {   
    val deps = new HashSet[Int]()   /*  change variable and method names from parent to deps
                                     *  because, RDG needs same variables, but child not parent
                                     */

    val name = _name
    val callSite = _callSite
    var count = 0
<<<<<<< HEAD
=======
    var isHadoopRDD = false
>>>>>>> LOSIC

    def addDeps(p: Int): Unit = {
      deps.add(p)
    }
    def addDeps(p: HashSet[Int]): Unit = {
      p.map(x => deps.add(x))
    }
    override def toString(): String = {
      val countStr = if (count != 0) s" Count: ${count}" else ""
      s"${name}(${callSite}) Dep:${deps}${countStr}"
    }
  }
  
<<<<<<< HEAD
  class CandidatesInfo(val _opId: String) {
=======
  class CandidatesInfo(val _opId: String) extends Cloneable {
>>>>>>> LOSIC
    val opId = _opId
    var inputSize: Double = 0
    var resultSize: Double = 0
    var time: Double = 0
    var counts: Int = 0
    var isCached: Boolean = false
<<<<<<< HEAD
    val parents = new HashSet[String]()
=======
    var parents = new HashSet[String]()

    override def clone(): CandidatesInfo = super.clone().asInstanceOf[CandidatesInfo]
>>>>>>> LOSIC

    def setCached(_isCached: Boolean): Unit = {
      isCached = _isCached
    }

    def addParents(p: String): Unit = {
      parents.add(p)
    }

    def addParents(p: HashSet[String]): Unit = {
      p.foreach(x => parents.add(x))
    }

    def isKnown: Boolean = {
      time match {
        case 0 => false
        case _ => true
      }
    }

    override def toString: String = {
<<<<<<< HEAD
      //s"$opId, InputSize:$inputSize, ResultSize:$resultSize, Time:$time, Counts:$counts, $isCached, $parents"
=======
>>>>>>> LOSIC
      s"$opId($inputSize, $resultSize, $time) x$counts, $isCached, $parents"
    }
  }

<<<<<<< HEAD
  //var opTimePerInAccum = HashSet[(String, Long, Long)]()
  //var opOutPerInAccum = HashSet[(String, Long, Long)]()
  private[spark] var candidatesOnStage = (LinkedHashSet[CandidatesInfo](), LinkedHashSet[CandidatesInfo]())
  // candidatesOnStage._1 is a set of persist candidates and ._2 is a set of unpersist candidates.

=======
  private[spark] var candidatesOnStage = LinkedHashSet[CandidatesInfo]()
  
>>>>>>> LOSIC
  class CandidatesCombination {
      val nodes = new HashSet[String]()
      var benefit: Double = 0
      var sumSize: Double = 0

      override def toString: String = {
        s"Nodes: ${nodes.mkString(", ")} Benefit: $benefit, SumSize: $sumSize"
      }
  }

<<<<<<< HEAD
=======
  def makeClusterWithPruning(ancestors: LinkedHashSet[CandidatesInfo]): Unit = {
    val visited = HashSet[CandidatesInfo]()

    def deleteNode(ancestors: LinkedHashSet[CandidatesInfo], node: CandidatesInfo): Unit = {
      val parents = node.parents
      val children = ancestors.filter(_.parents.contains(node.opId))

      children.foreach { child =>
        child.parents = parents
        child.time += node.time
      }
      ancestors.remove(node)
    }

    def self(node: CandidatesInfo, minSize: Double, counts: Int, cluster: LinkedHashSet[CandidatesInfo])
            : Unit = {
      visited.add(node)
      var _minSize = minSize
      if (node.counts == counts) {
        cluster.add(node)
        if (node.resultSize >= minSize) {
          _minSize = node.resultSize
          deleteNode(ancestors, node)
        }
      }
      val parents = node.parents.flatMap(y => ancestors.find(_.opId == y))
                    .filterNot(x => visited.contains(x))
      parents.foreach(self(_, _minSize, counts, cluster))
    }

    val candidatesLeaves = ancestors.filter{ x =>
      val parentsWithSameCounts = x.parents.flatMap(y => ancestors.find(_.opId == y).filter(_.counts == x.counts))
      val childrenWithSameCounts = ancestors.filter(_.parents.contains(x.opId)).filter(_.counts == x.counts)
      if (parentsWithSameCounts.nonEmpty
        && parentsWithSameCounts.size == x.parents.size
        && childrenWithSameCounts.size == 0) {
        true
      }
      else {
        false
      }
    }

    candidatesLeaves.foreach { leaf =>
      val cluster = new LinkedHashSet[CandidatesInfo]
      val minSize = leaf.resultSize
      self(leaf, minSize, leaf.counts, cluster)
    }
  }

>>>>>>> LOSIC
  def regression_powerModel(opName: String, 
                              readed: HashSet[(String, Double, Double)])
                             : (Double, Double, Double) = {
    import breeze.linalg._
    import breeze.stats.regression._
    import breeze.numerics._
    import breeze.stats._

<<<<<<< HEAD
    // if opName is a predictable , functional , constant....
    
=======
>>>>>>> LOSIC
    val x = DenseVector(readed.toSeq.map(_._2).toArray)
    val y = DenseVector(readed.toSeq.map(_._3).toArray)

    val logX = breeze.numerics.log(x)
    val logY = breeze.numerics.log(y)

    val X = DenseMatrix.horzcat(logX.asDenseMatrix.t, DenseMatrix.ones[Double](logX.length, 1))

    val leastSquaresResult = leastSquares(X, logY)
    val coefficients = leastSquaresResult.coefficients

    val a = math.exp(coefficients(1))
    val b = coefficients(0)

    val yPred = x.map(xi => a * math.pow(xi, b))

    val ssTotal = sum((y - mean(y)) ^:^ 2.0)
    val ssRes = sum((y - yPred) ^:^ 2.0)
    val rSquared = 1 - (ssRes / ssTotal)

    logInfoSSP(s"Power model for $opName: y = $a * x^$b with R^2 $rSquared", isSSparkLogEnabled)
    (a, b, rSquared)  // coefficient a and b
  }

  def regression_linearModel(opName: String, 
                              readed: HashSet[(String, Double, Double)])
                            : (Double, Double, Double) = {
    import breeze.linalg._
    import breeze.stats.regression._
    import breeze.numerics._
    import breeze.stats._

    val x = DenseVector(readed.toSeq.map(_._2).toArray)
    val y = DenseVector(readed.toSeq.map(_._3).toArray)

    val X = DenseMatrix.horzcat(x.asDenseMatrix.t, DenseMatrix.ones[Double](x.length, 1))

    val leastSquaresResult = leastSquares(X, y)
    val coefficients = leastSquaresResult.coefficients

    val a = coefficients(0)
    val b = coefficients(1)
    
    val yPred = x.map(xi => a * xi + b)

    val ssTotal = sum((y - mean(y)) ^:^ 2.0)
    val ssRes = sum((y - yPred) ^:^ 2.0)
    val rSquared = 1 - (ssRes / ssTotal)

    logInfoSSP(s"Linear model for $opName: " +
      s"y = $a * x + $b with R^2 $rSquared", isSSparkLogEnabled)
    (a, b, rSquared)  // coefficient a and b
  }

  def regression_polynomial2dModel(opName: String, 
                                    readed: HashSet[(String, Double, Double)])
                                  : (Double, Double, Double, Double) = {
    import breeze.linalg._
    import breeze.stats.regression._
    import breeze.numerics._
    import breeze.stats._
    
    val x = DenseVector(readed.toSeq.map(_._2).toArray)
    val y = DenseVector(readed.toSeq.map(_._3).toArray)

    val X = DenseMatrix.horzcat(
      x.map(xi => xi * xi).asDenseMatrix.t, 
      x.asDenseMatrix.t,                    
      DenseMatrix.ones[Double](x.length, 1) 
    )
    val leastSquaresResult = leastSquares(X, y)
    val coefficients = leastSquaresResult.coefficients

    val a2 = coefficients(0)
    val a1 = coefficients(1)
    val b = coefficients(2)

    val yPred = x.map(xi => a2 * xi * xi + a1 * xi + b)

    val ssTotal = sum((y - mean(y)) ^:^ 2.0)
    val ssRes = sum((y - yPred) ^:^ 2.0)
    val rSquared = 1 - (ssRes / ssTotal)

    logInfoSSP(s"Polynomial2d model for $opName: " +
      s"y = $a2 * x^2 + $a1 * x + $b with R^2 $rSquared", isSSparkLogEnabled)
    (a2, a1, b, rSquared) // coefficient a2, a1, and b
  }
  
  def regression_polynomial3dModel(opName: String, 
                                    readed: HashSet[(String, Double, Double)])
                                  : (Double, Double, Double, Double, Double) = {
    import breeze.linalg._
    import breeze.stats.regression._
    import breeze.numerics._
    import breeze.stats._
    
    val x = DenseVector(readed.toSeq.map(_._2).toArray)
    val y = DenseVector(readed.toSeq.map(_._3).toArray)

    val X = DenseMatrix.horzcat(
      x.map(xi => xi * xi * xi).asDenseMatrix.t, 
      x.map(xi => xi * xi).asDenseMatrix.t,      
      x.asDenseMatrix.t,                         
      DenseMatrix.ones[Double](x.length, 1)      
    )
    val leastSquaresResult = leastSquares(X, y)
    val coefficients = leastSquaresResult.coefficients

    val a3 = coefficients(0)
    val a2 = coefficients(1)
    val a1 = coefficients(2)
    val b = coefficients(3)

    val yPred = x.map(xi => a3 * xi * xi * xi + a2 * xi * xi + a1 * xi + b)

    val ssTotal = sum((y - mean(y)) ^:^ 2.0)
    val ssRes = sum((y - yPred) ^:^ 2.0)
    val rSquared = 1 - (ssRes / ssTotal)

    logInfoSSP(s"Polynomial3d model for $opName: " +
      s"y = $a3 * x^3 + $a2 * x^2 + $a1 * x + $b with R^2 $rSquared", isSSparkLogEnabled)
    (a3, a2, a1, b, rSquared) // coefficient a3, a2, a1, and b
  }

  def y_powerModel(coeffA: Double, coeffB: Double, x: Double)
                  : Double = {
    //xi => a * math.pow(xi, b)
    coeffA * math.pow(x, coeffB)
  }
  def y_linearModel(coeffA: Double, coeffB: Double, x: Double)
                    : Double = {
    //xi => a * xi + b
    coeffA * x + coeffB
  }
  def y_polynomial2dModel(coeffA2: Double, coeffA1: Double, coeffB: Double, x: Double)
                          : Double = {
    //xi => a2 * xi * xi + a1 * xi + b
    coeffA2 * x * x + coeffA1 * x + coeffB
  }
  def y_polynomial3dModel(coeffA3: Double, coeffA2: Double, coeffA1: Double, coeffB: Double, x: Double): 
                          Double = {
    //xi => a3 * xi * xi * xi + a2 * xi * xi + a1 * xi + b
    coeffA3 * x * x * x + coeffA2 * x * x + coeffA1 * x + coeffB
  }

  // opId = opName(newCallSite), sizeOrTime = "size" or "time"
  def readProfiling(opId: String, sizeOrTime: String): (String, HashSet[(String, Double, Double)]) = { 
    val opName = opId.split('(')(0)
    val filePath = Paths.get(profilingDir + "/" + opName + "_" + sizeOrTime + ".csv")
    val readed = HashSet[(String, Double, Double)]()
    if (Files.exists(filePath)) {
      val fileReader = Source.fromFile(filePath.toString)
      for (line <- fileReader.getLines()) {
        val tmpTokens = line.split(',')
        val candName = tmpTokens(0)//,filterNot(_.isWhitespace)
        val firstItem = tmpTokens(1).toDouble   // input size
        val secondItem = tmpTokens(2).toDouble  // time from _time, output size from _size
        readed.add( (candName, firstItem, secondItem) ) // duplicated value will be eliminated
      }
      fileReader.close()
    }
    (opName, readed)
  }

  def makeOpId(rddId: Int): String = {
    val localLineage = lineages.values.flatMap(_.values).find(_.contains(rddId))
    if (localLineage.isDefined) {
      val node = localLineage.get.get(rddId)
      s"${node.get.name}(${node.get.callSite})"
    }
    else {
      logErrorSSP(s"Cannot get node ${rddId}")
      ""
    }
  }

  def makeCandidatesOnStage(candidatesOnLineage: LinkedHashMap[Int, LineageInfo])
<<<<<<< HEAD
    : (LinkedHashSet[CandidatesInfo], LinkedHashSet[CandidatesInfo]) = {
    val result = (LinkedHashSet[CandidatesInfo](), LinkedHashSet[CandidatesInfo]())
=======
    : LinkedHashSet[CandidatesInfo] = {
    val result = LinkedHashSet[CandidatesInfo]()
>>>>>>> LOSIC
    val leafNode = candidatesOnLineage.head._1

    def self(rddId: Int, result: LinkedHashSet[CandidatesInfo]): Unit = {
      candidatesOnLineage.find(x => x._1 == rddId) match {
        case Some(node) =>
          val opId = makeOpId(node._1)
          val parents = node._2.deps
          val parentsId = HashSet[String]()
          parents.filterNot(x => x == -1 || x == -2 || x == -3).foreach{ x =>
            parentsId.add(makeOpId(x))
          }
          cacheCandidates.find(x => x.opId == opId && x.parents.equals(parentsId)) match {
            case Some(candidate) =>
              result += candidate
            case None =>
          }
          parents.filter(x => candidatesOnLineage.contains(x)).foreach(self(_, result))
        case None =>
      }
    }
<<<<<<< HEAD
    self(leafNode, result._1)
    result._1.foreach { x =>
      x.inputSize = 0
      x.resultSize = 0
      x.time = 0
      val remainingCounts = x.counts - 1
      if (unpersistPlan) {
        if (remainingCounts > -1) {
          x.counts = remainingCounts
        }
        else {
          result._1.remove(x)
          result._2.add(x)
        }
      }
      else {
=======

    self(leafNode, result)
    
    result.foreach { x =>
      val remainingCounts = x.counts - 1
      if (remainingCounts >= 0) {
>>>>>>> LOSIC
        x.counts = remainingCounts
      }
    }
    result
  }

  def getValueFromRegression(readed: HashSet[(String, Double, Double)], 
                              opName: String, 
                              inputSize: Double, 
                              sizeOrTime: String): Double = {
    var resultValue: Double = 0 // this value should be time or resultSize
<<<<<<< HEAD
=======
    logInfoSSP(s"getValueFromRegression: try to get $sizeOrTime of $opName with input $inputSize", isSSparkLogEnabled)
>>>>>>> LOSIC
    try {
      if (readed.size > 1) {
        logInfoSSP(s"Try to makeRegressionModel LINEAR $sizeOrTime for $opName", isSSparkLogEnabled)
        val (a, b, rSquared) = regression_linearModel(opName, readed)
        if (rSquared < 0.75) {
          logInfoSSP(s"Try to makeRegressionModel POLY2DIM for $sizeOrTime $opName", isSSparkLogEnabled)
          val (a2, a1, b, rSquared) = regression_polynomial2dModel(opName, readed)
          if (rSquared < 0.75) {
            logInfoSSP(s"Try to makeRegressionModel POLY3DIM for $sizeOrTime $opName", isSSparkLogEnabled)
            val (a3, a2, a1, b, rSquared) = regression_polynomial3dModel(opName, readed)
            resultValue = y_polynomial3dModel(a3, a2, a1, b, inputSize)
          }
          else {
            resultValue = y_polynomial2dModel(a2, a1, b, inputSize)
          }
        }
        else {
<<<<<<< HEAD
          resultValue = y_linearModel(a, b, inputSize)
=======
          resultValue = y_linearModel(a, b, inputSize) 
>>>>>>> LOSIC
        }
      }
    } catch {
      case e: Exception =>
<<<<<<< HEAD
        isSSparkOptimizeEnabled = false
        logWarningSSP(s"cannot find a regressionModel for ${opName}... OptimizeEnabled has set false")
    }
    if (resultValue < 0) {
      logWarningSSP(s"Predicted Y $sizeOrTime value of $opName is $resultValue -> 0", isSSparkLogEnabled)
      resultValue = 0
=======
        //isSSparkOptimizeEnabled = false
        logWarningSSP(s"cannot find a regressionModel for ${opName}... ") //OptimizeEnabled has set false")
        if (sizeOrTime == "size")
          resultValue = inputSize * 0.01
        else if (sizeOrTime == "time")
          resultValue = 0
    }
    if (resultValue < 0) {
      if (sizeOrTime == "size") {
        logWarningSSP(s"Predicted Y $sizeOrTime value of $opName is $resultValue -> ${inputSize * 0.01}", isSSparkLogEnabled)
        resultValue = inputSize * 0.01
      }
      else if (sizeOrTime == "time") {
        logWarningSSP(s"Predicted Y $sizeOrTime value of $opName is $resultValue -> 0", isSSparkLogEnabled)
        resultValue = 0
      }
>>>>>>> LOSIC
    }
    else {
      logInfoSSP(s"Predicted Y $sizeOrTime value of $opName  = $resultValue", isSSparkLogEnabled)
    }
    
    resultValue
  }

  def setSizeOfCandidates(ancestors: LinkedHashSet[CandidatesInfo], stageInput: Long): Unit = { // Top-down
    val visited = HashSet[CandidatesInfo]()
    def self(node: CandidatesInfo): Unit = {
      visited.add(node)
      val op = node.opId.split('(')(0)
      var resultSize: Double = 0
      val (opName, readedSize) = readProfiling(node.opId, "size")
        readedSize ++= dagScheduler.opOutPerIn.filter(x => x._1.startsWith(opName+"("))
                                .map{x => 
                                  logInfoSSP(s"added accumulator size info. ${(x._1, x._2.toFloat / Math.pow(2,20), x._3.toFloat / Math.pow(2,20))}", isSSparkLogEnabled)
                                  (x._1, x._2.toFloat / Math.pow(2,20), x._3.toFloat / Math.pow(2,20))
                                }

      if (functionalOps.contains(op)) { 
        val readedWithSameOpId = readedSize.filter(_._1 == node.opId)
        resultSize = getValueFromRegression(readedWithSameOpId, opName, node.inputSize, "size")
      }
      /* Don't need to distinguish constantOps 
      else if (constantOps.contains(op)){
        resultSize = getValueFromRegression(readedSize, opName, node.inputSize, "size")
      } */
      else {  // predictable
        resultSize = getValueFromRegression(readedSize, opName, node.inputSize, "size")
      }

      node.resultSize = resultSize  
      val children = ancestors.filter(x => x.parents.contains(node.opId)).filterNot(x => visited.contains(x))
<<<<<<< HEAD
      logInfoSSP(s"Node visited ${node.opId} -> ${children.map(_.opId)}")
      children.foreach { x =>   
        x.inputSize += resultSize
=======
      logInfoSSP(s"Node visited ${node.opId} -> ${children.map(_.opId)}", isSSparkLogEnabled)
      children.foreach { x =>   
        x.inputSize = resultSize
>>>>>>> LOSIC
        self(x)
      }
    }

    val rootNode = ancestors.find(x => x.opId == makeOpId(rootLineage.last))
      // ancestors.find(_.parents.isEmpty)
    rootNode match {
      case Some(x) =>
        x.inputSize = stageInput / Math.pow(2,20)
        self(x)
      case None => 
        logErrorSSP(s"Cannot find root node in persist candidates RDD id ${rootLineage.last}")
    }
  }

  // Do not need Top-down, because time can be calculated by inputSize of itself
  def setTimeOfCandidates(ancestors: LinkedHashSet[CandidatesInfo]): Unit = {
    ancestors.foreach{ node =>
      val op = node.opId.split('(')(0)
      var resultTime: Double = 0
      val (opName, readedTime) = readProfiling(node.opId, "time")
      readedTime ++= dagScheduler.opTimePerIn.filter(x => x._1.startsWith(opName+"("))
                              .map{x => 
                                logInfoSSP(s"added accumulator time info. ${(x._1, x._2.toFloat / Math.pow(2,20), x._3.toFloat / 1e9)}", isSSparkLogEnabled)
                                (x._1, x._2.toFloat / Math.pow(2,20), x._3.toFloat / 1e9)
                              }

      if (functionalOps.contains(op)) { 
        //HashSet[(String, Double, Double)
        val readedWithSameOpId = readedTime.filter(_._1 == node.opId)
        resultTime = getValueFromRegression(readedWithSameOpId, opName, node.inputSize, "time")
      }
      else if (constantOps.contains(op)){
        resultTime = readedTime.maxBy(_._3)._3
      } 
      else {  // predictable
        resultTime = getValueFromRegression(readedTime, opName, node.inputSize, "time")
      }
      node.time = resultTime
    }
  }
  
  def findTopNodes(ancestors: LinkedHashSet[CandidatesInfo]): LinkedHashSet[CandidatesInfo] = {
    ancestors.filter(node => node.parents.isEmpty)
  }

<<<<<<< HEAD
=======
  /*
>>>>>>> LOSIC
  def calculateBenefitFromTopDown(
                                    node: CandidatesInfo,
                                    selectedNodes: HashSet[String],
                                    allNodes: LinkedHashSet[CandidatesInfo],
                                    benefit: Double,
                                    currentTime: Double
                                  ): Double = {
    var accumulatedBenefit = benefit
    var accumulatedTime = currentTime + node.time

    if (selectedNodes.contains(node.opId)) {
      accumulatedBenefit += accumulatedTime * node.counts 
      // counts has reduced already by calling makeCandidatesOnStage() (getAncestors())
      accumulatedTime = 0.0
    }
    for (child <- allNodes.filter(_.parents.contains(node.opId))) {
<<<<<<< HEAD
=======
      logInfoSSP(s"calculateBenefit: ${node.opId} => ${child.opId}")
>>>>>>> LOSIC
      accumulatedBenefit = calculateBenefitFromTopDown(child, selectedNodes, allNodes, 
                                                      accumulatedBenefit, accumulatedTime)
    }
    accumulatedBenefit
<<<<<<< HEAD
=======
  }*/

  def calculateBenefitFromTopDown(
                                 node: CandidatesInfo,
                                 selectedNodes: HashSet[String],
                                 allNodes: LinkedHashSet[CandidatesInfo],
                                 benefit: Double,
                                 currentTime: Double,
                                 timeMap: HashMap[String, Double] = HashMap(),
                                 visited: HashSet[String] = HashSet()
                               ): Double = {

    if (visited.contains(node.opId)) {
      return benefit
    }

    visited.add(node.opId) 

    var accumulatedBenefit = benefit
    val newTime = currentTime + node.time

    if (selectedNodes.contains(node.opId)) {
      accumulatedBenefit += newTime * node.counts
      timeMap.update(node.opId, 0.0)
    } else {
      timeMap.update(node.opId, timeMap.getOrElse(node.opId, 0.0) + newTime)
    }

    for (child <- allNodes.filter(_.parents.contains(node.opId))) {
      //logInfoSSP(s"calculateBenefit: ${node.opId} => ${child.opId}")
      accumulatedBenefit = calculateBenefitFromTopDown(
        child,
        selectedNodes,
        allNodes,
        accumulatedBenefit,
        timeMap(node.opId),
        timeMap,
        visited 
      )
    }
    accumulatedBenefit
>>>>>>> LOSIC
  }

  def generateCombinations(ancestors: LinkedHashSet[CandidatesInfo]): HashSet[CandidatesCombination] = {
    val combinations = new HashSet[CandidatesCombination]()
    val topNodes = findTopNodes(ancestors)
    val totalCombinations = 1 << ancestors.size // 2^n
    val ancestorsList = ancestors.toList

    for (i <- 0 until totalCombinations) {
      val combination = new CandidatesCombination()

      for (j <- ancestorsList.indices) {
        if ((i & (1 << j)) != 0) {
          val node = ancestorsList(j)
          combination.nodes.add(node.opId)
          combination.sumSize += node.resultSize
        }
      }

      for (topNode <- topNodes) {
        combination.benefit += calculateBenefitFromTopDown(topNode, combination.nodes, ancestors, 0.0, 0.0)
      }
      combinations.add(combination)
    }
    combinations
  }

  def findBestCombination(combinations: HashSet[CandidatesCombination], 
                          remainingMem: Double): Option[CandidatesCombination] = {
    val validCombinations = combinations.filter(_.sumSize <= remainingMem)
    
    if (validCombinations.nonEmpty) {
      Some(validCombinations.maxBy(c => (c.benefit, -c.sumSize))) // maximize benefit, minimize sumSize
    } else {
      None
    }
  }
<<<<<<< HEAD
    
  def selectCandidates(stageId: Int, stageInput: Long): Unit = {
    @deprecated // inner function of getAncestors
    def findAncestors(nodeId: String, result: LinkedHashSet[CandidatesInfo]): Unit = {
      cacheCandidates.find(_.opId == nodeId) match {
        case Some(candidate) =>
          if (!result.contains(candidate)) {
            result += candidate
            candidate.parents.foreach(parentId => findAncestors(parentId, result))
          }
        case None =>
      }
    }

    @deprecated // getAncestors has some error for handling duplicated opIds, replaced it with makeCandidatesOnStage()
    def getAncestors(startNodeId: String): LinkedHashSet[CandidatesInfo] = {
      val result = LinkedHashSet[CandidatesInfo]()
      findAncestors(startNodeId, result)
      result.foreach { x =>
        x.inputSize = 0
        x.resultSize = 0
        x.time = 0
        x.counts -= 1
      }
      result
    }

    // start point of selectCandidates
=======

  def makeCacheListViaMimicRun(rdg: HashMap[Int, LineageInfo], cacheCandidates: LinkedHashSet[CandidatesInfo]): Unit = {
    val actionNodes: LinkedHashMap[Int, LineageInfo] = LinkedHashMap(rdg.filter(_._2.name == "Action").toSeq.sortBy(_._1): _*)
    val jobVisited = new LinkedHashSet[String]
    val visitedCandidatesOnJob = new HashSet[LinkedHashSet[CandidatesInfo]]
    var predictedMem: Double = getRemainingStorageMem() / Math.pow(2,20)

    def makeCandidatesOnJob(opSeqOnJob: LinkedHashSet[String]): LinkedHashSet[CandidatesInfo] = {
      val candidatesOnJob: LinkedHashSet[CandidatesInfo] = cacheCandidates.filter(x => opSeqOnJob.contains(x.opId))
      candidatesOnJob.foreach(_.counts -= 1)

      val unpersistList = candidatesOnJob.filter(_.counts <= 0)
      predictedMem += unpersistList.toSeq.map(_.resultSize).sum

      candidatesOnJob
    }

    def self(nodeKey: Int): Unit = {
      val opId = s"${rdg.get(nodeKey).get.name}(${rdg.get(nodeKey).get.callSite})"
      jobVisited.add(opId)
      val parents = rdg.filter(x => x._2.deps.contains(nodeKey))
      parents.foreach( x => self(x._1))
    }

    actionNodes.foreach { action =>
      jobVisited.clear()
      self(action._1)

      val candidatesOnJob = makeCandidatesOnJob(jobVisited)

      if (!visitedCandidatesOnJob.contains(candidatesOnJob) && candidatesOnJob.nonEmpty) {
        visitedCandidatesOnJob.add(candidatesOnJob)

        val maxSizeOfCandidates: Double = candidatesOnJob.toSeq.map(_.resultSize).sum
        if (predictedMem >= maxSizeOfCandidates) {
          cacheList ++= candidatesOnJob.map(_.opId).toSet.to[HashSet]
          predictedMem -= maxSizeOfCandidates
        }
        else {  // insufficient memory
          val combinations = generateCombinations(candidatesOnJob)
          val bestCombination = findBestCombination(combinations, predictedMem)
          unpersistPlan = true
          setUnpersistPlan(unpersistPlan)
          
          bestCombination match {
            case Some(combination) =>
              cacheList ++= combination.nodes
              predictedMem -= combination.sumSize
            case None =>
              
          }
        }
      }
    }
    logInfoSSP(s"Cache List => $cacheList", isSSparkLogEnabled)
  }

  def selectCandidates(stageInput: Long): Unit = {  // Mature case, only stage 0
    val remainingMem = getRemainingStorageMem() / Math.pow(2,20)
    val localCacheCandidates: LinkedHashSet[CandidatesInfo] = cacheCandidates.map(_.clone())
    
    val opIdSet = localCacheCandidates.map(_.opId).toSet
    val nonParentsNodes = localCacheCandidates
      .filterNot { candidate => 
        candidate.parents.forall(opIdSet.contains)  
      }
      .flatMap { candidate => 
        readedRDG.find {
          case (key, lineageInfo) =>
            val tmpOpId = s"${lineageInfo.name}(${lineageInfo.callSite})"
            tmpOpId == candidate.opId
        }.map(_._1)
      }.to[HashSet]
    
    nonParentsNodes.foreach { x => 
      val parents = readedRDG.filter(y => y._2.deps.contains(x)).map(_._1).toSet.to[HashSet]
      
      def self(rdd: Int): Unit = {
        val tmpOpId = s"${readedRDG(rdd).name}(${readedRDG(rdd).callSite})"
        if (localCacheCandidates.find(_.opId == tmpOpId).isEmpty) {
          val parents = readedRDG.filter(y => y._2.deps.contains(rdd))
          val parentsOpIds = parents.map(x => s"${x._2.name}(${x._2.callSite})").toSet.to[HashSet]
          if (!(parentsOpIds.size == 1 && parentsOpIds.last == tmpOpId)) {
            localCacheCandidates.add(new CandidatesInfo(tmpOpId))
            localCacheCandidates.last.counts = readedRDG(rdd).count
            localCacheCandidates.last.addParents(parentsOpIds)
            logInfoSSP(s"Node added for candidate's parents: Node $tmpOpId with paretns $parentsOpIds", isSSparkLogEnabled)
          }
          parents.map(_._1).toSet.to[HashSet].foreach(self(_))
        }
      }
      parents.foreach(self(_))
    }

    setSizeOfCandidates(localCacheCandidates, stageInput)
    setTimeOfCandidates(localCacheCandidates)
    val originCandidates = localCacheCandidates.filter(_.counts >= 2)
    makeClusterWithPruning(originCandidates)
    logInfoSSP(s"#candidates after pruning = ${originCandidates.size}")
    makeCacheListViaMimicRun(readedRDG, originCandidates)
  }

  def selectCandidates(stageId: Int): Unit = { // Mature case, for every stage
>>>>>>> LOSIC
    val lineage = getLineage(stageId)

    val candidatesOnLineage = lineage.filter { x =>
      val opId = makeOpId(x._1)
      cacheCandidates.exists(x => x.opId == opId)
    }
    logInfoSSP(s"candidatesOnLineage $candidatesOnLineage", isSSparkLogEnabled)

    if (candidatesOnLineage.size == 0) {
      logInfoSSP("No candidates on this stage", isSSparkLogEnabled)
    }
    else {
<<<<<<< HEAD
      //val leafCand = makeOpId(candidatesOnLineage.head._1)
      //val candidatesOnStage = getAncestors(leafCand)
      candidatesOnStage = makeCandidatesOnStage(candidatesOnLineage)
      logInfoSSP(s"persist candidates ${candidatesOnStage._1} with leafCand ${makeOpId(candidatesOnLineage.head._1)}", isSSparkLogEnabled)

      if ( candidatesOnStage._1.size != 0 ){   /** PageRank has this case, lineage like ( map <- distinct <- distinct <- distinct )
                                                in Stage x with lineage ( distinct <- distinct )
                                                the case of calling OP itself again was eliminated because of preventing infinite loop
                                                So that in this case, candidatesOnLineage has distinct as a candidate,
                                                but candidatesOnStage doesn't have.
                                                However, in the previous stage, (map<-distinct) node is already scheduled as a candidate
                                                So, it seems fine to skip for the (distinct<-distinct)
                                            */
        setSizeOfCandidates(candidatesOnStage._1, stageInput)
        val remainingMem = getRemainingStorageMem() / Math.pow(2,20)
        val maxSizeOfCandidates: Double = candidatesOnStage._1.toSeq.map(_.resultSize).sum
        var localCacheList = HashSet[String]()
        
        // if there is no overhead, does it really need to divide into sufficient or insufficient case?
        // it's hard to measure the accuracy of benefit in case of sufficient memory
        if (remainingMem >= maxSizeOfCandidates) {  // sufficient memory
          if (cachePlan == "refreshing")
            cacheList = candidatesOnStage._1.map(_.opId).toSet.to[HashSet]
          else if (cachePlan == "enhancing" || cachePlan == "pruning")
            localCacheList = candidatesOnStage._1.map(_.opId).toSet.to[HashSet]
          logInfoSSP(s"Sufficient mem ${remainingMem}MB: " +
            s"All candidates on stage ${cacheList.mkString(", ")} " +
            s"SumSize: ${maxSizeOfCandidates}MB", isSSparkLogEnabled)
        }
      
        else {  // insufficient memory
          setTimeOfCandidates(candidatesOnStage._1)
          val combinations = generateCombinations(candidatesOnStage._1)
          logInfoSSP(s"Combinations ${combinations}", isSSparkLogEnabled)
          val bestCombination = findBestCombination(combinations, remainingMem)
        
          bestCombination match {
            case Some(combination) =>
              if (cachePlan == "refreshing")
                cacheList = combination.nodes
              else if (cachePlan == "enhancing" || cachePlan == "pruning")
                localCacheList = combination.nodes
              logInfoSSP(s"Insufficient mem ${remainingMem}MB: " +
                s"Best combination found ${combination.nodes.mkString(", ")} " +
                s"Benefit: ${combination.benefit} and SumSize: ${combination.sumSize}MB", isSSparkLogEnabled)
            case None =>
              if (cachePlan == "refreshing")
                cacheList = HashSet()
              else if (cachePlan == "enhancing" || cachePlan == "pruning")
                localCacheList = HashSet()
              logErrorSSP(s"Insufficient mem ${remainingMem}MB: " +
                s"No valid combination found within the remaining memory.")
          }
        }
        if (cachePlan == "enhancing")
          cacheList ++= localCacheList
        else if (cachePlan == "pruning") {
          val unselectedNodes = candidatesOnStage._1.map(_.opId).toSet.to[HashSet].diff(localCacheList)
          cacheList --= unselectedNodes
        }
      }
      else {
        logInfoSSP("No candidates on this stage or the candidates already have scheduled", isSSparkLogEnabled)
      }
    }

    logInfoSSP(s"Candidates List=> $cacheCandidates", isSSparkLogEnabled)
    logInfoSSP(s"Cache List => $cacheList")
    //val remainingMem = getRemainingStorageMem()
    //logInfoSSP(s"MEM: $remainingMem", isSSparkLogEnabled)
=======
      candidatesOnStage = makeCandidatesOnStage(candidatesOnLineage)
    }
    logInfoSSP(s"Candidates List on Stage $stageId=> $cacheCandidates", isSSparkLogEnabled)
    logInfoSSP(s"Cache List => $cacheList", isSSparkLogEnabled)
  }

  val visitedCandidatesOnStage = new HashSet[LinkedHashSet[CandidatesInfo]]

  def selectCandidates(stageId: Int, stageInput: Long): Unit = { // Immature case, for every stage
    val lineage = getLineage(stageId)

    val candidatesOnLineage = lineage.filter { x =>
      val opId = makeOpId(x._1)
      cacheCandidates.exists(x => x.opId == opId)
    }
    logInfoSSP(s"candidatesOnLineage $candidatesOnLineage", isSSparkLogEnabled)

    if (candidatesOnLineage.size == 0) {
      logInfoSSP("No candidates on this stage", isSSparkLogEnabled)
    }
    else {
      candidatesOnStage = makeCandidatesOnStage(candidatesOnLineage)

      if (!visitedCandidatesOnStage.contains(candidatesOnStage) && candidatesOnStage.nonEmpty) {
        visitedCandidatesOnStage.add(candidatesOnStage)

        logInfoSSP(s"persist candidates ${candidatesOnStage} with leafCand ${makeOpId(candidatesOnLineage.head._1)}", isSSparkLogEnabled)

        if ( candidatesOnStage.size != 0 ){   /** PageRank has this case, lineage like ( map <- distinct <- distinct <- distinct )
                                                  in Stage x with lineage ( distinct <- distinct )
                                                  the case of calling OP itself again was eliminated because of preventing infinite loop
                                                  So that in this case, candidatesOnLineage has distinct as a candidate,
                                                  but candidatesOnStage doesn't have.
                                                  However, in the previous stage, (map<-distinct) node is already scheduled as a candidate
                                                  So, it seems fine to skip for the (distinct<-distinct)
                                              */
          setSizeOfCandidates(candidatesOnStage, stageInput)
          val remainingMem = getRemainingStorageMem() / Math.pow(2,20)
          val maxSizeOfCandidates: Double = candidatesOnStage.toSeq.map(_.resultSize).sum
          var localCacheList = HashSet[String]()
          
          // if there is no overhead, does it really need to divide into sufficient or insufficient case?
          // it's hard to measure the accuracy of benefit in case of sufficient memory
          if (remainingMem >= maxSizeOfCandidates) {  // sufficient memory
            //candidatesOnStage._1.foreach(_.isCached = true)
            if (cachePlan == "refreshing")
              cacheList = candidatesOnStage.map(_.opId).toSet.to[HashSet]
            else if (cachePlan == "enhancing" || cachePlan == "pruning")
              localCacheList = candidatesOnStage.map(_.opId).toSet.to[HashSet]
            logInfoSSP(s"Sufficient mem ${remainingMem}MB: " +
              s"All candidates on stage ${cacheList.mkString(", ")} " +
              s"SumSize: ${maxSizeOfCandidates}MB", isSSparkLogEnabled)
          }
        
          else {  // insufficient memory
            logInfoSSP(s"Insufficient mem ${remainingMem}MB: " +
              s"All candidates on stage ${cacheList.mkString(", ")} " +
              s"SumSize: ${maxSizeOfCandidates}MB", isSSparkLogEnabled)
            if (!unpersistForceDisabled){
							unpersistPlan = true
  	          setUnpersistPlan(unpersistPlan)
						}
            
            setTimeOfCandidates(candidatesOnStage)
            val combinations = generateCombinations(candidatesOnStage)
            logInfoSSP(s"Combinations ${combinations}", isSSparkLogEnabled)
            val bestCombination = findBestCombination(combinations, remainingMem)
          
            bestCombination match {
              case Some(combination) =>
                /*
                candidatesOnStage._1.foreach { x => 
                  if (combination.nodes.contains(x.opId)) {
                    x.isCached = true
                  }
                }*/
                if (cachePlan == "refreshing")
                  cacheList = combination.nodes
                else if (cachePlan == "enhancing" || cachePlan == "pruning")
                  localCacheList = combination.nodes
                logInfoSSP(s"Insufficient mem ${remainingMem}MB: " +
                  s"Best combination found ${combination.nodes.mkString(", ")} " +
                  s"Benefit: ${combination.benefit} and SumSize: ${combination.sumSize}MB", isSSparkLogEnabled)
              case None =>
                if (cachePlan == "refreshing")
                  cacheList = HashSet()
                else if (cachePlan == "enhancing" || cachePlan == "pruning")
                  localCacheList = HashSet()
                logErrorSSP(s"Insufficient mem ${remainingMem}MB: " +
                  s"No valid combination found within the remaining memory.")
            }
          }
          if (cachePlan == "enhancing")
            cacheList ++= localCacheList
          else if (cachePlan == "pruning") {
            val unselectedNodes = candidatesOnStage.map(_.opId).toSet.to[HashSet].diff(localCacheList)
            cacheList --= unselectedNodes
          }
        }
        else {
          logInfoSSP("No candidates on this stage or the candidates already have scheduled", isSSparkLogEnabled)
        }
      }
      else {
        logInfoSSP("selectCandidates is skipped, keep candidates", isSSparkLogEnabled)
      }
    }

    logInfoSSP(s"Candidates List on Stage $stageId=> $cacheCandidates", isSSparkLogEnabled)
    logInfoSSP(s"Cache List => $cacheList", isSSparkLogEnabled)
>>>>>>> LOSIC
  }
  
  // use LinkedHashMap for ordering of lineage
  val lineages = HashMap[Int, HashMap[Int, LinkedHashMap[Int, LineageInfo]]]() // [ JobId, [ StageId, Lineage ] ]
<<<<<<< HEAD
  val rootLineage = HashSet[Int]()  // SSPARK TODO: it doens't need to be HashSet, just a scalar value
  val rootName = HashSet[String]()
  
=======
  val rootLineage = HashSet[Int]()
  //val rootName = HashSet[String]()
  //val rddCheck = HashMap[Int, (Boolean, Boolean)]() // K = rddId, V1 = isCachableHadoopRDD, V2 = isUncachableHadoopRDD
  val hadoopRDDs = HashSet[Int]()
  var countFunctional: Int = _

>>>>>>> LOSIC
  def putLineage(jobId: Int, stageId: Int, value: LinkedHashMap[Int, LineageInfo]): Unit = {
    lineages.getOrElseUpdate(jobId, HashMap()).put(stageId, value)
  }

  def getLineage(stageId: Int): LinkedHashMap[Int, LineageInfo] = {
    lineages.values.flatMap(_.get(stageId)).headOption match {
      case Some(x) => x
      case None =>
        logErrorSSP(s"$stageId lineage does not exist")
        new LinkedHashMap[Int, LineageInfo]
    }
  }

  def checkLineage(lineage: LinkedHashMap[Int, LineageInfo]): Int = { 
    var countAll = 0
    lineage.foreach{ x =>
      if (x._2.deps.contains(-1))
        countAll += 1
      if (x._2.deps.contains(-2))
        countAll += 1
    }
    countAll
  }

  def getRootLineage(lineage: LinkedHashMap[Int, LineageInfo]): Int = {
    val rootCandidates = lineage.filter(x => x._2.deps.contains(-1) || x._2.deps.contains(-2) || x._2.deps.contains(-3))
    if (rootCandidates.size > 0) {
      if ( rootCandidates.size == 1 ) {
        rootCandidates.head._1
      }
      else {
        rootCandidates.minBy(x => x._1)._1
      }
    }
    else {
      logErrorSSP("cannot find a root")
      0
    }
  }
<<<<<<< HEAD

  @deprecated // this is old version of getRootLineage, need to check new version can handle all cases
  def getRootLineage_depr(lineage: LinkedHashMap[Int, LineageInfo]): Int = {
    val rootLineage =
      if (checkLineage(lineage) == 1)   // Job start (-2) or shuffle start (-1)
        lineage.find(x => x._2.deps.contains(-1) | x._2.deps.contains(-2)).get._1
      else {
        // logErrorSSP(s"it has multiple starting nodes $checkLineage")
        // ALS encounter this case, I'm not sure that this case only can be made from cached case?
        // Now, if there are multiple root and there isn't no cached intermediate cached RDD, then it occurs runtime error
        val rootNodes = lineage.filter(x => x._2.deps.contains(-1) || x._2.deps.contains(-2))
                              .filter(x => x._2.deps.size == 1)
        if (rootNodes.size == 1) {
          rootNodes.head._1
        }
        else if (lineage.filter(x => x._2.deps.contains(-3)).size == 1){  //for ParallelCollectionRDD
          lineage.filter(x => x._2.deps.contains(-3)).head._1
        }
        else if (lineage.filter(x => x._2.deps.contains(-1)).size > 1) {  //for multiple ShuffledRDD
          if (lineage.filter(x => x._2.deps.contains(-1)).filter(x => x._2.deps.size == 1).size > 1) {
            lineage.filter(x => x._2.deps.contains(-1)).filter(x => x._2.deps.size == 1).minBy(x => x._1)._1
          }
          else {
            lineage.filter(x => x._2.deps.contains(-1)).minBy(x => x._1)._1
          }
          //update makeLineage wih over stage dependency for ShuffledRDD
        }
        else { // I'm not sure that it is exist like this case, brute forcely set 0
          // ALS cannot find a root, because it's start with ParallelCollectionRDD
          logErrorSSP("cannot find a root")
          0
        }
      }
    rootLineage
  }
=======
>>>>>>> LOSIC
  
  def getOpName(rdd: RDD[_]): String = {  // maybe don't need anymore
    val tmpStr = rdd.toString.split(" at ")
    if (tmpStr.length == 3) {
      val opName = tmpStr(1).filterNot(_.isWhitespace)
      val callSite = tmpStr(2).split('[')(0).filterNot(_.isWhitespace)
      opName + "(" + callSite + ")"
    }
    else {
      logErrorSSP("cannot get OpName")
      ""
    }
  }
 
<<<<<<< HEAD
=======
  /* SSPARK: These operations have RDDs in pairs, such as rdd 0 and rdd 1.
    Usually the first RDD is hadoopRDD, and the second RDD is mapPatitionsRDD.
    And the first rdd is always unmeasurable in size, so it is combined into one RDD (usually rdd 1).
  */
  def isPairHadoopRDD(op: String): Boolean = {
    if (op == "objectFile" ||
        op == "sequenceFile" ||
        op == "textFile") {
        true
    }
    else
      false
  }

>>>>>>> LOSIC
  /**
   * SSPARK: Make a lineage for each task or job
   * To adopt an optimization approach, a lineage must be created for each task.
   */ 
  def makeLineage(jobId: Int, stageId: Int, rdd: RDD[_]): Unit = {
    val lineage = new LinkedHashMap[Int, LineageInfo]()
    def selfInfo(rdd: RDD[_]): Unit = {
      val len = rdd.dependencies.length
<<<<<<< HEAD
      len match{
=======
      len match {
>>>>>>> LOSIC
        case 0 => 
          val id = addNode(rdd)
          if (rdd.toString.startsWith("ParallelCollectionRDD")) lineage.get(id).get.addDeps(-3) 
          else lineage.get(id).get.addDeps(-2)  // from Job
        case 1 =>
          val parentDep = rdd.dependencies.head
          if (parentDep.isInstanceOf[ShuffleDependency[_, _, _]]) {
            //logInfoSSP(s"$rdd has parent: -1(Stage)")
            val id = addNode(rdd)
            //lineage.get(id).get.addDeps(-1)  // from Stage
            lineage.get(id).get.addDeps(HashSet(-1, parentID(parentDep))) 
          }
          else {
            //logInfoSSP(s"$rdd has parent: ${parentDep.rdd}")
            val id = addNode(rdd)
            lineage.get(id).get.addDeps(parentID(parentDep))
            selfInfo(parentDep.rdd)
          }
        case _ =>
          //logInfoSSP(s"$rdd has $len parents: ")
          val id = addNode(rdd)
          rdd.dependencies.foreach{parentDep => 
            if (parentDep.isInstanceOf[ShuffleDependency[_, _, _]]) {
              //logInfoSSP(s"${parentDep.rdd}\t\t -1(Stage)")
              //lineage.get(id).get.addDeps(-1)
              lineage.get(id).get.addDeps(HashSet(-1, parentID(parentDep))) 
            }
            else {
              //logInfoSSP(s"\t\t ${parentDep.rdd}")
              lineage.get(id).get.addDeps(parentID(parentDep))
              selfInfo(parentDep.rdd)
            }
          }
      }
    }

<<<<<<< HEAD
    @deprecated //Dep should be recognized in the previous step, so this method may handle only 1 dep
    def parentsIDs(parentDep: Seq[Dependency[_]]): HashSet[Int] = {
      var ids = new HashSet[Int]()
      parentDep.map{x => 
        val tmpStr = x.rdd.toString.split(" at ")
        val id = tmpStr(0).split('[')(1).split(']')(0).toInt
        ids.add(id)
      }
      ids
    }

=======
>>>>>>> LOSIC
    def parentID(parentDep: Dependency[_]): Int = { 
      val tmpStr = parentDep.rdd.toString.split(" at ")
      val id = tmpStr(0).split('[')(1).split(']')(0).toInt
      id
    }

    def addNode(rdd: RDD[_]): Int = {
      val id = rdd.id
      val tmpStr = rdd.newCreationSite.split('(')
      if (tmpStr.length == 2){
        val opName = tmpStr(0)
        val callSite = tmpStr(1).replace(")","")
        if (!lineage.contains(id)) {
          lineage.put(id, new LineageInfo(opName, callSite))
<<<<<<< HEAD
=======

          val rString = rdd.toString
          if (rString.startsWith("hdfs://") ||
              rString.startsWith("HadoopRDD") ||
              rString.startsWith("NewHadoopRDD") ||
              rdd.newCreationSite.startsWith("sequenceFile") ) {
              /* sequenceFile usually has hadoopRDD in the first RDD 
                 and mapPartitionsRDD in the second RDD, 
                 but both cannot be cached (and sized) and must be treated as hadoopRDD. */
            lineage.get(id).get.isHadoopRDD = true
          }
>>>>>>> LOSIC
          //rdd.optimizeCache()  // for making a new decision whether cache it or not, really need this?
        }
      }
      else logErrorSSP(s"Lineage addNode unknwon case ${rdd}")
      id      
    }

<<<<<<< HEAD
    @deprecated   // previous version, without colummn
    def addNode_depr(rdd: RDD[_]): Int = {
      var id = -1
      val tmpStr = rdd.toString.split(" at ")
      if (tmpStr.length == 3) {
        id = tmpStr(0).split('[')(1).split(']')(0).toInt
        val opName = tmpStr(1).filterNot(_.isWhitespace)
        val callSite = tmpStr(2).split('[')(0).filterNot(_.isWhitespace)
        if (lineage.contains(id)) None
        else lineage.put(id, new LineageInfo(opName, callSite))
      }
      else logErrorSSP(s"Lineage addNode unknwon case ${rdd}")
      id
    }

    //lineage.clear()
    
    rootLineage.clear()
    rootName.clear()
    selfInfo(rdd)
    //lineages.put(stageId, lineage)
    putLineage(jobId, stageId, lineage)
    rootLineage.add(getRootLineage(lineage))
    rootName.add(lineage(getRootLineage(lineage)).name)
    logInfoSSP(s"Lineage was made with root ${rootLineage} $lineage \n ${rdd.toDebugString} \n" +
=======
    rootLineage.clear()
    //rootName.clear()
    //rddCheck.clear()
    hadoopRDDs.clear()
    
    selfInfo(rdd)
    /*
    lineage.foreach{ x => 
      val rddName = x._2.name
      rddCheck.put( x._1, (isCachableHadoopRDD(rddName), isUncachableHadoopRDD(rddName)) ) 
    }*/

    hadoopRDDs ++= lineage.filter(_._2.isHadoopRDD).keys
    logInfoSSP (s" hadoopRDDs  ${hadoopRDDs}", isSSparkLogEnabled)

    putLineage(jobId, stageId, lineage)
    rootLineage.add(getRootLineage(lineage))
    //rootName.add(lineage(getRootLineage(lineage)).name)
    logInfoSSP(s"Lineage was made with root ${rootLineage} on Stage @$stageId@ Job @$jobId@ @$lineage@ \n ${rdd.toDebugString} \n" +
>>>>>>> LOSIC
      s"Job $jobId Stage $stageId cachePlan [$cachePlan] cacheList: $cacheList"
    , isSSparkLogEnabled)
  }
  
  def checkOptimizeCache(rdd: RDD[_]): Unit = { //, candidatesOnStage: LinkedHashSet[CandidatesInfo]): Unit = {
    def selfInfo(rdd: RDD[_]): Unit ={
      val len = rdd.dependencies.length
      len match{
        case 0 =>
          addPlan(rdd)
        case 1 =>
          val parentDep = rdd.dependencies.head
          if (parentDep.isInstanceOf[ShuffleDependency[_, _, _]]) {
            addPlan(rdd)
          }
          else {
            addPlan(rdd)
            selfInfo(parentDep.rdd)
          }
        case _ =>
          addPlan(rdd)
          rdd.dependencies.foreach{parentDep =>
            if (!parentDep.isInstanceOf[ShuffleDependency[_, _, _]]) {
              selfInfo(parentDep.rdd)
            }
          }
      }
    }
    def addPlan(rdd: RDD[_]): Unit = {
<<<<<<< HEAD
      if (candidatesOnStage._1.find(x => x.opId == rdd.newCreationSite).isDefined) {
        rdd.optimizeCache()
      }
      else if (candidatesOnStage._2.find(x => x.opId == rdd.newCreationSite).isDefined)
      {
        rdd.unpersist()
        logInfoSSP(s"unpersist ${rdd.id} at ${rdd.newCreationSite}", isSSparkLogEnabled)
      }
=======
      candidatesOnStage.find(x => x.opId == rdd.newCreationSite && !x.isCached) match {
        case Some(candidate) =>
          //candidate.isCached = true
          logInfoSSP(s"Cache RDD ${rdd.id} at ${rdd.newCreationSite}", isSSparkLogEnabled)
          rdd.optimizeCache()
        case None =>
          // do nothing
      }
    }
    selfInfo(rdd)
  }

  def checkOptimizeUnpersist(rdd: RDD[_]): Unit = { 
    def selfInfo(rdd: RDD[_]): Unit ={
      val len = rdd.dependencies.length
      len match{
        case 0 =>
          addPlan(rdd)
        case 1 =>
          val parentDep = rdd.dependencies.head
          if (parentDep.isInstanceOf[ShuffleDependency[_, _, _]]) {
            addPlan(rdd)
          }
          else {
            addPlan(rdd)
            selfInfo(parentDep.rdd)
          }
        case _ =>
          addPlan(rdd)
          rdd.dependencies.foreach{parentDep =>
            if (!parentDep.isInstanceOf[ShuffleDependency[_, _, _]]) {
              selfInfo(parentDep.rdd)
            }
          }
      }
    }
    
    def addPlan(rdd: RDD[_]): Unit = {
      cacheCandidates.filter(x => x.isCached && x.counts == 0).find(_.opId == rdd.newCreationSite) match {
        case Some(candidate) =>
          rdd.unpersist()
          candidate.isCached = false
          logInfoSSP(s"unpersist RDD ${rdd.id} at ${rdd.newCreationSite}", isSSparkLogEnabled)
        case None =>
          // Do nothing
      }
      /* old version
      if (candidatesOnStage._2.find(x => x.opId == rdd.newCreationSite).isDefined) {
        rdd.unpersist()
        candidatesOnStage._2.find(x => x.opId == rdd.newCreationSite).get.isCached = false
        //candidatesOnStage._2.remove(candidatesOnStage._2.find(x => x.opId == rdd.newCreationSite).get)
        logInfoSSP(s"unpersist RDD ${rdd.id} at ${rdd.newCreationSite}", isSSparkLogEnabled)
      }
      */
>>>>>>> LOSIC
    }
    selfInfo(rdd)
  }

<<<<<<< HEAD
  /*
  private[spark] var newAppName: String = _

  def setNewAppName(rdd: RDD[_]): Unit = {
    val tmpStr = rdd.toString.split(" at ")
    if (tmpStr.length == 3) {
      newAppName = tmpStr(2).split('.')(0).filterNot(_.isWhitespace)
    }
    else {
      logErrorSSP("SSparkOptimize set disabled because it's not able to recognize appname")
      isSSparkOptimizeEnabled = false
    }
  }*/
=======
  var profiledCount = 0
  var computedCount = 0
  val profileRDDs = HashSet[Int]()

  def makeProfilePlanner(lineage: LinkedHashMap[Int, LineageInfo], rootLineage: Int, stageInputBytes: Long)
                          : Unit = {
    profileRDDs.clear()
    val profilePlanner = HashSet[Int]()
    val visited = HashSet[Int]()
    
    def self(node: Int, depth: Int, inputSize: Double): Unit = {  // Top-Down
      val opId = lineage(node).name + "(" + lineage(node).callSite + ")"
      val deps = lineage(node).deps
      val children = lineage.filter(_._2.deps.contains(node))

      val sameOp: Boolean =
        deps.exists { id =>
          lineage.get(id).exists { v =>
            opId == s"${v.name}(${v.callSite})"
          }
        }
      visited.add(node)

      if (!sameOp) {
        var (opName, readedSize) = readProfiling(opId, "size")

        if (functionalOps.contains(opName)) {
          readedSize = readedSize.filter(_._1 == opId)
        }
        
        val exists = readedSize.exists { case (_, second, _) =>
          val percentError = 1 - Math.pow(profileThreshold, depth)
          math.abs(second - inputSize) <= inputSize * percentError
        }

        if (!exists) {
          profilePlanner.add(node)
        }
        
        val predSize = getValueFromRegression(readedSize, opName, inputSize, "size")

        children.foreach { x => 
          if (!visited.contains(x._1)) {
            self(x._1, depth+1, predSize)
          }
        }
      }
      else {  //if there is same operation parents then it should be profiled, already in profilePlanner
        if (deps.exists(profilePlanner.contains)) {
          profilePlanner.add(node)
        }
        children.foreach { x =>
          if (!visited.contains(x._1)) {
            self(x._1, depth, inputSize)  // same depth and inputSize, because it should be into same operation.
          }
        }
      }
    }

    val startInputSize: Double = stageInputBytes.toFloat / Math.pow(2,20)
    self(rootLineage, 0, startInputSize)
    
    // need to add parents to know input size.
    val current = profilePlanner.toSet
    for (node <- current) {
      lineage.get(node).foreach { info =>
        profilePlanner ++= info.deps.filter(_ >= 0)
      }
    }
    
    profileRDDs ++= profilePlanner
    
    logInfoSSP(s"profilePlanner has been made -> ${profileRDDs}", isSSparkLogEnabled)
    /*
    val iProfilePlanner: scala.collection.immutable.HashSet[Int] =
            scala.collection.immutable.HashSet.empty ++ profilePlanner
    iProfilePlanner
    */
  }
>>>>>>> LOSIC

  def getFileNameFromRootRDD(rdg: HashMap[Int, LineageInfo]): String = {
    rdg.get(rdg.minBy(_._1)._1).get.callSite.split('(')(0).split('.')(0) + ".rdg"
  }

  def makeRDG(lineages: HashMap[Int, HashMap[Int, LinkedHashMap[Int, LineageInfo]]]): HashMap[Int, LineageInfo] = {
    val rdg = HashMap[Int, LineageInfo]()

    def findChildren(rdd: Int): HashSet[Int] = {
      val children = HashSet[Int]()
      lineages.foreach { jobLineage =>
        jobLineage._2.foreach { stageLineage =>
          val nodeInfo = stageLineage._2.filter(x => x._2.deps.contains(rdd))
          nodeInfo.foreach(x => children.add(x._1))
        }
      }
      children
    }

    def addNode(rdd: Int): Unit = {
      val node = lineages.values.flatMap(_.values).find(_.contains(rdd)).get.find(_._1 == rdd).get
      rdg.put(rdd, new LineageInfo(node._2.name, node._2.callSite))
      val children = findChildren(rdd)
      rdg.get(rdd).get.addDeps(children)
    }

    def selfInfo(rdd: Int): Unit = {
      if (!rdg.contains(rdd)) {
        addNode(rdd)
        findChildren(rdd).map(x => selfInfo(x))
      }
    }

<<<<<<< HEAD
    def findParent(rdd: Int): HashSet[Int] = {
      val parent = HashSet[Int]()
      rdg.filter(x => x._2.deps.contains(rdd)).map(x => parent.add(x._1))
      parent
    }

    def countActions(rdd: Int): Unit = {
      rdg.get(rdd).get.count = rdg.get(rdd).get.count+1
      findParent(rdd).map(countActions(_))
    }

=======
>>>>>>> LOSIC
    def addActionLineages(): Unit = {
      val maxRdd = lineages.values.flatMap(_.values).flatMap(_.keys).max + 1

      for (jobId <- 0 to lineages.size - 1) {
        val lastStage = lineages(jobId).maxBy(_._1)._1
        val lineage = getLineage(lastStage)
        val lastNode = lineage.head._1
        lineage.put(maxRdd + jobId, new LineageInfo("Action", s"Job$jobId"))
        lineage.get(maxRdd + jobId).get.addDeps(lastNode)
      }
    }
<<<<<<< HEAD
    
    addActionLineages()
    val root = lineages.minBy(_._1)._2.minBy(_._1)._2.minBy(_._1)._1 // normally it should be 0
    selfInfo(root)
    val leafNodes = HashSet[Int]()
    rdg.filter(_._2.deps.size == 0).foreach(x => leafNodes.add(x._1))

    leafNodes.map(countActions(_))
=======

    def countNodes(): Unit = {
      rdg.foreach { x =>
        val count = lineages.values
          .flatMap(_.values)
          .flatMap(_.keys)
          .count(_ == x._1)
        rdg.get(x._1).get.count = count
      }
    }

    addActionLineages()
    val root = lineages.minBy(_._1)._2.minBy(_._1)._2.minBy(_._1)._1 // normally it should be 0
    selfInfo(root)
    countNodes()

>>>>>>> LOSIC
    rdg
  }

  def writeRDG(rdg: HashMap[Int, LineageInfo]): Unit = {
    val filePath = Paths.get(rdgDir) + "/" + newAppName + ".rdg"//getFileNameFromRootRDD(rdg)
    val fw = new FileWriter(filePath)

    rdg.foreach(x => fw.write(s"${x._1}; ${x._2.name}; ${x._2.callSite}; ${x._2.deps}; ${x._2.count}\n"))

    fw.close()
  }

  def readRDG(): HashMap[Int, LineageInfo] = {
    val rdg = HashMap[Int, LineageInfo]()
    val filePath = Paths.get(rdgDir) + "/" + newAppName + ".rdg"
    if (existsRDG(newAppName)) {
      val fileReader = Source.fromFile(filePath)
      def toDepsSet(deps: String): HashSet[Int] = {
        val depsStr = deps.replace("Set(","")
                          .replace(")","")
                          .replace(" ","")
        val depsSet = HashSet[Int]()
        depsStr.split(',').map(x => depsSet.add(x.toInt))
        depsSet
      }
      for (line <- fileReader.getLines()) {
        val tmpLine = line.split(';')
        val rddId: Int = tmpLine(0).filterNot(_.isWhitespace).toInt
        val opName: String = tmpLine(1).filterNot(_.isWhitespace)
        val callSite: String = tmpLine(2).filterNot(_.isWhitespace)
        val deps: HashSet[Int] = {
          val depsTmp = tmpLine(3).filterNot(_.isWhitespace)
          if (depsTmp == "Set()")
            HashSet[Int]()
          else
            toDepsSet(depsTmp)
        }
        val count: Int = tmpLine(4).filterNot(_.isWhitespace).toInt
        rdg.put(rddId, {val info = new LineageInfo(opName, callSite)
          info.addDeps(deps)
          info.count = count
          info})
      }
      fileReader.close()
    }
<<<<<<< HEAD
    else {
      logErrorSSP("Cannot read RDG, SSparkOptimize set disabled")
      isSSparkOptimizeEnabled = false
=======
    else {     
      isSSparkOptimizeEnabled = false
      setOptimizeEnabled(isSSparkOptimizeEnabled)
      //lastOptimizeFlag = false

      logWarningSSP(s"Cannot read RDG, SSparkOptimize is disabled.")
>>>>>>> LOSIC
    }
    rdg
  }
  
  def existsRDG(appName: String): Boolean = {   //appName -> without .scala:#line.... e.g., "SVMWithSGDExample"
<<<<<<< HEAD
    //val filePath = Paths.get(rdgDir + "/" + appName.split('.')(0) + ".rdg")
=======
>>>>>>> LOSIC
    val filePath = Paths.get(rdgDir + "/" + appName + ".rdg")
    if (Files.exists(filePath)) true
    else false
  }

<<<<<<< HEAD
=======
  var minRemainingStorageMem = Long.MaxValue
>>>>>>> LOSIC
  def getRemainingStorageMem(): Long = {
    val storageStatus = SparkEnv.get.blockManager.master.getStorageStatus
    if (isSSparkLogEnabled) {
      storageStatus.foreach { status =>
         logInfoSSP(s"Executor ID: ${status.blockManagerId.executorId}, " +
         s"Max Memory: ${status.maxMem}, Remaining Memory: ${status.memRemaining}")
      }
    }
<<<<<<< HEAD
    storageStatus.filter(_.blockManagerId.executorId != "driver").map(_.memRemaining).sum
  }

  /*
  def profilingTrim(dirPath: String): Unit = {
    val dir = new File(dirPath)
    val profilingFiles = dir.listFiles.filter(file => file.isFile && file.getName.endsWith(".csv"))

    profilingFiles.foreach { file =>
      val lines = Source.fromFile(file).getLines().toList
      if (lines.length > maxProfiling) {
        val trimmedLines = lines.takeRight(maxProfiling)
        val writer = new PrintWriter(file)
        trimmedLines.foreach(writer.println)
        writer.close()
      }
    }
  }*/

=======
    val remainingMem = storageStatus.filter(_.blockManagerId.executorId != "driver").map(_.memRemaining).sum
    if (isSSparkMemCheck && minRemainingStorageMem > remainingMem && remainingMem != 0) 
      minRemainingStorageMem = remainingMem
    remainingMem
  }

>>>>>>> LOSIC
  def profilingTrim(dirPath: String): Unit = {
    val dir = new File(dirPath)
    val profilingFiles = dir.listFiles.filter(file => file.isFile && file.getName.endsWith(".csv"))

    profilingFiles.foreach { file =>
      val lines = Source.fromFile(file).getLines().toList

      // Parse lines into structured data
      val parsedData = lines.map { line =>
        val parts = line.split(",").map(_.trim)
        (parts(0), parts(1).toDouble, parts(2).toDouble)
      }

      // Group by the first two elements and calculate the average of the third element
      val aggregatedData = parsedData
        .groupBy { case (key1, key2, _) => (key1, key2) }
        .map { case ((key1, key2), group) =>
          val avgThird = group.map(_._3).sum / group.size
          (key1, key2, avgThird)
        }
        .toList

<<<<<<< HEAD
      // Limit the number of lines to maxProfiling if necessary
      val trimmedData = if (aggregatedData.size > maxProfiling) {
        aggregatedData.takeRight(maxProfiling)
=======
      // Limit the number of lines to maxProfile if necessary
      val trimmedData = if (aggregatedData.size > maxProfile) {
        aggregatedData.takeRight(maxProfile)
>>>>>>> LOSIC
      } else {
        aggregatedData
      }

      // Write back to the file
      val writer = new PrintWriter(file)
      try {
        trimmedData.foreach { case (key1, key2, avgThird) =>
          writer.println(s"$key1, $key2, $avgThird")
        }
      } finally {
        writer.close()
      }
    }
  }

  /**
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function. This is the main entry point for all actions in Spark.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
   * @param resultHandler callback to pass each result to
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    accumScope.clear()  // SSPARK
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array.
   * The function that is run against each partition additionally takes `TaskContext` argument.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
   * @return in-memory collection with a result of the job (each collection element will contain
   * a result from one partition)
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
   * @return in-memory collection with a result of the job (each collection element will contain
   * a result from one partition)
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]): Array[U] = {
    val cleanedFunc = clean(func)
    runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array. The function
   * that is run against each partition additionally takes `TaskContext` argument.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @return in-memory collection with a result of the job (each collection element will contain
   * a result from one partition)
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @return in-memory collection with a result of the job (each collection element will contain
   * a result from one partition)
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function. The function
   * that is run against each partition additionally takes `TaskContext` argument.
   *
   * @param rdd target RDD to run tasks on
   * @param processPartition a function to run on each partition of the RDD
   * @param resultHandler callback to pass each result to
   */
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    processPartition: (TaskContext, Iterator[T]) => U,
    resultHandler: (Int, U) => Unit)
  {
    runJob[T, U](rdd, processPartition, 0 until rdd.partitions.length, resultHandler)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function.
   *
   * @param rdd target RDD to run tasks on
   * @param processPartition a function to run on each partition of the RDD
   * @param resultHandler callback to pass each result to
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit)
  {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.length, resultHandler)
  }

  /**
   * :: DeveloperApi ::
   * Run a job that can return approximate results.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator `ApproximateEvaluator` to receive the partial results
   * @param timeout maximum time to wait for the job, in milliseconds
   * @return partial result (how partial depends on whether the job was finished before or
   * after timeout)
   */
  @DeveloperApi
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long): PartialResult[R] = {
    assertNotStopped()
    val callSite = getCallSite
    logInfo("Starting job: " + callSite.shortForm)
    val start = System.nanoTime
    val cleanedFunc = clean(func)
    val result = dagScheduler.runApproximateJob(rdd, cleanedFunc, evaluator, callSite, timeout,
      localProperties.get)
    logInfo(
      "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

  /**
   * Submit a job for execution and return a FutureJob holding the result.
   *
   * @param rdd target RDD to run tasks on
   * @param processPartition a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   * partitions of the target RDD, e.g. for operations like `first()`
   * @param resultHandler callback to pass each result to
   * @param resultFunc function to be executed when the result is ready
   */
  def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): SimpleFutureAction[R] =
  {
    assertNotStopped()
    val cleanF = clean(processPartition)
    val callSite = getCallSite
    val waiter = dagScheduler.submitJob(
      rdd,
      (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
      partitions,
      callSite,
      resultHandler,
      localProperties.get)
    new SimpleFutureAction(waiter, resultFunc)
  }

  /**
   * Submit a map stage for execution. This is currently an internal API only, but might be
   * promoted to DeveloperApi in the future.
   */
  private[spark] def submitMapStage[K, V, C](dependency: ShuffleDependency[K, V, C])
      : SimpleFutureAction[MapOutputStatistics] = {
    assertNotStopped()
    val callSite = getCallSite()
    var result: MapOutputStatistics = null
    val waiter = dagScheduler.submitMapStage(
      dependency,
      (r: MapOutputStatistics) => { result = r },
      callSite,
      localProperties.get)
    new SimpleFutureAction[MapOutputStatistics](waiter, result)
  }

  /**
   * Cancel active jobs for the specified group. See `org.apache.spark.SparkContext.setJobGroup`
   * for more information.
   */
  def cancelJobGroup(groupId: String) {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId)
  }

  /** Cancel all jobs that have been scheduled or are running.  */
  def cancelAllJobs() {
    assertNotStopped()
    dagScheduler.cancelAllJobs()
  }

  /**
   * Cancel a given job if it's scheduled or running.
   *
   * @param jobId the job ID to cancel
   * @param reason optional reason for cancellation
   * @note Throws `InterruptedException` if the cancel message cannot be sent
   */
  def cancelJob(jobId: Int, reason: String): Unit = {
    dagScheduler.cancelJob(jobId, Option(reason))
  }

  /**
   * Cancel a given job if it's scheduled or running.
   *
   * @param jobId the job ID to cancel
   * @note Throws `InterruptedException` if the cancel message cannot be sent
   */
  def cancelJob(jobId: Int): Unit = {
    dagScheduler.cancelJob(jobId, None)
  }

  /**
   * Cancel a given stage and all jobs associated with it.
   *
   * @param stageId the stage ID to cancel
   * @param reason reason for cancellation
   * @note Throws `InterruptedException` if the cancel message cannot be sent
   */
  def cancelStage(stageId: Int, reason: String): Unit = {
    dagScheduler.cancelStage(stageId, Option(reason))
  }

  /**
   * Cancel a given stage and all jobs associated with it.
   *
   * @param stageId the stage ID to cancel
   * @note Throws `InterruptedException` if the cancel message cannot be sent
   */
  def cancelStage(stageId: Int): Unit = {
    dagScheduler.cancelStage(stageId, None)
  }

  /**
   * Kill and reschedule the given task attempt. Task ids can be obtained from the Spark UI
   * or through SparkListener.onTaskStart.
   *
   * @param taskId the task ID to kill. This id uniquely identifies the task attempt.
   * @param interruptThread whether to interrupt the thread running the task.
   * @param reason the reason for killing the task, which should be a short string. If a task
   *   is killed multiple times with different reasons, only one reason will be reported.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(
      taskId: Long,
      interruptThread: Boolean = true,
      reason: String = "killed via SparkContext.killTaskAttempt"): Boolean = {
    dagScheduler.killTaskAttempt(taskId, interruptThread, reason)
  }

  /**
   * Clean a closure to make it ready to be serialized and sent to tasks
   * (removes unreferenced variables in $outer's, updates REPL variables)
   * If <tt>checkSerializable</tt> is set, <tt>clean</tt> will also proactively
   * check to see if <tt>f</tt> is serializable and throw a <tt>SparkException</tt>
   * if not.
   *
   * @param f the closure to clean
   * @param checkSerializable whether or not to immediately check <tt>f</tt> for serializability
   * @throws SparkException if <tt>checkSerializable</tt> is set but <tt>f</tt> is not
   *   serializable
   * @return the cleaned closure
   */
  private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed.
   * @param directory path to the directory where checkpoint files will be stored
   * (must be HDFS path if running in cluster)
   */
  def setCheckpointDir(directory: String) {

    // If we are running on a cluster, log a warning if the directory is local.
    // Otherwise, the driver may attempt to reconstruct the checkpointed RDD from
    // its own local file system, which is incorrect because the checkpoint files
    // are actually on the executor machines.
    if (!isLocal && Utils.nonLocalPaths(directory).isEmpty) {
      logWarning("Spark is not running in local mode, therefore the checkpoint directory " +
        s"must not be on the local filesystem. Directory '$directory' " +
        "appears to be on the local filesystem.")
    }

    checkpointDir = Option(directory).map { dir =>
      val path = new Path(dir, UUID.randomUUID().toString)
      val fs = path.getFileSystem(hadoopConfiguration)
      fs.mkdirs(path)
      fs.getFileStatus(path).getPath.toString
    }
  }

  def getCheckpointDir: Option[String] = checkpointDir

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD). */
  def defaultParallelism: Int = {
    assertNotStopped()
    taskScheduler.defaultParallelism
  }

  /**
   * Default min number of partitions for Hadoop RDDs when not given by user
   * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
   * The reasons for this are discussed in https://github.com/mesos/spark/pull/718
   */
  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

  private val nextShuffleId = new AtomicInteger(0)

  private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()

  private val nextRddId = new AtomicInteger(0)

  /** Register a new RDD, returning its RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

  /**
   * Registers listeners specified in spark.extraListeners, then starts the listener bus.
   * This should be called after all internal listeners have been registered with the listener bus
   * (e.g. after the web UI and event logging listeners have been registered).
   */
  private def setupAndStartListenerBus(): Unit = {
    try {
      conf.get(EXTRA_LISTENERS).foreach { classNames =>
        val listeners = Utils.loadExtensions(classOf[SparkListenerInterface], classNames, conf)
        listeners.foreach { listener =>
          listenerBus.addToSharedQueue(listener)
          logInfo(s"Registered listener ${listener.getClass().getName()}")
        }
      }
    } catch {
      case e: Exception =>
        try {
          stop()
        } finally {
          throw new SparkException(s"Exception when registering SparkListener", e)
        }
    }

    listenerBus.start(this, _env.metricsSystem)
    _listenerBusStarted = true
  }

  /** Post the application start event */
  private def postApplicationStart() {
    // Note: this code assumes that the task scheduler has been initialized and has contacted
    // the cluster manager to get an application ID (in case the cluster manager provides one).
    listenerBus.post(SparkListenerApplicationStart(appName, Some(applicationId),
      startTime, sparkUser, applicationAttemptId, schedulerBackend.getDriverLogUrls))
  }

  /** Post the application end event */
  private def postApplicationEnd() {
    listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis))
  }

  /** Post the environment update event once the task scheduler is ready */
  private def postEnvironmentUpdate() {
    if (taskScheduler != null) {
      val schedulingMode = getSchedulingMode.toString
      val addedJarPaths = addedJars.keys.toSeq
      val addedFilePaths = addedFiles.keys.toSeq
      val environmentDetails = SparkEnv.environmentDetails(conf, schedulingMode, addedJarPaths,
        addedFilePaths)
      val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails)
      listenerBus.post(environmentUpdate)
    }
  }

  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having finished construction.
  // NOTE: this must be placed at the end of the SparkContext constructor.
  SparkContext.setActiveContext(this, allowMultipleContexts)
}

/**
 * The SparkContext object contains a number of implicit conversions and parameters for use with
 * various Spark features.
 */
object SparkContext extends Logging {

  private val VALID_LOG_LEVELS =
    Set("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")

  /**
   * Lock that guards access to global variables that track SparkContext construction.
   */
  private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()

  /**
   * The active, fully-constructed SparkContext.  If no SparkContext is active, then this is `null`.
   *
   * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK.
   */
  private val activeContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  /**
   * Points to a partially-constructed SparkContext if some thread is in the SparkContext
   * constructor, or `None` if no SparkContext is being constructed.
   *
   * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK
   */
  private var contextBeingConstructed: Option[SparkContext] = None

  /**
   * Called to ensure that no other SparkContext is running in this JVM.
   *
   * Throws an exception if a running context is detected and logs a warning if another thread is
   * constructing a SparkContext.  This warning is necessary because the current locking scheme
   * prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
   */
  private def assertNoOtherContextIsRunning(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      Option(activeContext.get()).filter(_ ne sc).foreach { ctx =>
          val errMsg = "Only one SparkContext may be running in this JVM (see SPARK-2243)." +
            " To ignore this error, set spark.driver.allowMultipleContexts = true. " +
            s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
          val exception = new SparkException(errMsg)
          if (allowMultipleContexts) {
            logWarning("Multiple running SparkContexts detected in the same JVM!", exception)
          } else {
            throw exception
          }
        }

      contextBeingConstructed.filter(_ ne sc).foreach { otherContext =>
        // Since otherContext might point to a partially-constructed context, guard against
        // its creationSite field being null:
        val otherContextCreationSite =
          Option(otherContext.creationSite).map(_.longForm).getOrElse("unknown location")
        val warnMsg = "Another SparkContext is being constructed (or threw an exception in its" +
          " constructor).  This may indicate an error, since only one SparkContext may be" +
          " running in this JVM (see SPARK-2243)." +
          s" The other SparkContext was created at:\n$otherContextCreationSite"
        logWarning(warnMsg)
      }
    }
  }

  /**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
   *
   * @note This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
   * @param config `SparkConfig` that will be used for initialisation of the `SparkContext`
   * @return current `SparkContext` (or a new one if it wasn't created before the function call)
   */
  def getOrCreate(config: SparkConf): SparkContext = {
    // Synchronize to ensure that multiple create requests don't trigger an exception
    // from assertNoOtherContextIsRunning within setActiveContext
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(config), allowMultipleContexts = false)
      } else {
        if (config.getAll.nonEmpty) {
          logWarning("Using an existing SparkContext; some configuration may not take effect.")
        }
      }
      activeContext.get()
    }
  }

  /**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
   *
   * This method allows not passing a SparkConf (useful if just retrieving).
   *
   * @note This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
   * @return current `SparkContext` (or a new one if wasn't created before the function call)
   */
  def getOrCreate(): SparkContext = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(), allowMultipleContexts = false)
      }
      activeContext.get()
    }
  }

  /** Return the current active [[SparkContext]] if any. */
  private[spark] def getActive: Option[SparkContext] = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      Option(activeContext.get())
    }
  }

  /**
   * Called at the beginning of the SparkContext constructor to ensure that no SparkContext is
   * running.  Throws an exception if a running context is detected and logs a warning if another
   * thread is constructing a SparkContext.  This warning is necessary because the current locking
   * scheme prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
   */
  private[spark] def markPartiallyConstructed(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = Some(sc)
    }
  }

  /**
   * Called at the end of the SparkContext constructor to ensure that no other SparkContext has
   * raced with this constructor and started.
   */
  private[spark] def setActiveContext(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = None
      activeContext.set(sc)
    }
  }

  /**
   * Clears the active SparkContext metadata.  This is called by `SparkContext#stop()`.  It's
   * also called in unit tests to prevent a flood of warnings from test suites that don't / can't
   * properly clean up their SparkContexts.
   */
  private[spark] def clearActiveContext(): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      activeContext.set(null)
    }
  }

  private[spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private[spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private[spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  private[spark] val RDD_SCOPE_KEY = "spark.rdd.scope"
  private[spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

  /**
   * Executor id for the driver.  In earlier versions of Spark, this was `<driver>`, but this was
   * changed to `driver` because the angle brackets caused escaping issues in URLs and XML (see
   * SPARK-6716 for more details).
   */
  private[spark] val DRIVER_IDENTIFIER = "driver"

  /**
   * Legacy version of DRIVER_IDENTIFIER, retained for backwards-compatibility.
   */
  private[spark] val LEGACY_DRIVER_IDENTIFIER = "<driver>"

  private implicit def arrayToArrayWritable[T <: Writable : ClassTag](arr: Traversable[T])
    : ArrayWritable = {
    def anyToWritable[U <: Writable](u: U): Writable = u

    new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]],
        arr.map(x => anyToWritable(x)).toArray)
  }

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
   *
   * @param cls class that should be inside of the jar
   * @return jar that contains the Class, `None` if not found
   */
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class",
        // so pull out the /path/foo.jar
        Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Find the JAR that contains the class of a particular object, to make it easy for users
   * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
   * your driver program.
   *
   * @param obj reference to an instance which class should be inside of the jar
   * @return jar that contains the class of the instance, `None` if not found
   */
  def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)

  /**
   * Creates a modified version of a SparkConf with the parameters that can be passed separately
   * to SparkContext, to make it easier to write SparkContext's constructors. This ignores
   * parameters that are passed as the default value of null, instead of throwing an exception
   * like SparkConf would.
   */
  private[spark] def updatedConf(
      conf: SparkConf,
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map()): SparkConf =
  {
    val res = conf.clone()
    res.setMaster(master)
    res.setAppName(appName)
    if (sparkHome != null) {
      res.setSparkHome(sparkHome)
    }
    if (jars != null && !jars.isEmpty) {
      res.setJars(jars)
    }
    res.setExecutorEnv(environment.toSeq)
    res
  }

  /**
   * The number of cores available to the driver to use for tasks such as I/O with Netty
   */
  private[spark] def numDriverCores(master: String): Int = {
    numDriverCores(master, null)
  }

  /**
   * The number of cores available to the driver to use for tasks such as I/O with Netty
   */
  private[spark] def numDriverCores(master: String, conf: SparkConf): Int = {
    def convertToInt(threads: String): Int = {
      if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }
    master match {
      case "local" => 1
      case SparkMasterRegex.LOCAL_N_REGEX(threads) => convertToInt(threads)
      case SparkMasterRegex.LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
      case "yarn" =>
        if (conf != null && conf.getOption("spark.submit.deployMode").contains("cluster")) {
          conf.getInt("spark.driver.cores", 0)
        } else {
          0
        }
      case _ => 0 // Either driver is not being used, or its core count will be interpolated later
    }
  }

  /**
   * Create a task scheduler based on a given master URL.
   * Return a 2-tuple of the scheduler backend and the task scheduler.
   */
  private def createTaskScheduler(
      sc: SparkContext,
      master: String,
      deployMode: String): (SchedulerBackend, TaskScheduler) = {
    import SparkMasterRegex._

    // When running locally, don't try to re-execute tasks on failure.
    val MAX_LOCAL_TASK_FAILURES = 1

    master match {
      case "local" =>
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_REGEX(threads) =>
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        if (threadCount <= 0) {
          throw new SparkException(s"Asked to run locally with $threadCount threads")
        }
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*, M] means the number of cores on the computer with M failures
        // local[N, M] means exactly N threads with M failures
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
        // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
        val memoryPerSlaveInt = memoryPerSlave.toInt
        if (sc.executorMemory > memoryPerSlaveInt) {
          throw new SparkException(
            "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
              memoryPerSlaveInt, sc.executorMemory))
        }

        val scheduler = new TaskSchedulerImpl(sc)
        val localCluster = new LocalSparkCluster(
          numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
        val masterUrls = localCluster.start()
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
          localCluster.stop()
        }
        (backend, scheduler)

      case masterUrl =>
        val cm = getClusterManager(masterUrl) match {
          case Some(clusterMgr) => clusterMgr
          case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
        }
        try {
          val scheduler = cm.createTaskScheduler(sc, masterUrl)
          val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
          cm.initialize(scheduler, backend)
          (backend, scheduler)
        } catch {
          case se: SparkException => throw se
          case NonFatal(e) =>
            throw new SparkException("External scheduler cannot be instantiated", e)
        }
    }
  }

  private def getClusterManager(url: String): Option[ExternalClusterManager] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoaders =
      ServiceLoader.load(classOf[ExternalClusterManager], loader).asScala.filter(_.canCreate(url))
    if (serviceLoaders.size > 1) {
      throw new SparkException(
        s"Multiple external cluster managers registered for the url $url: $serviceLoaders")
    }
    serviceLoaders.headOption
  }
}

/**
 * A collection of regexes for extracting information from the master string.
 */
private object SparkMasterRegex {
  // Regular expression used for local[N] and local[*] master formats
  val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
  // Regular expression for local[N, maxRetries], used in tests with failing tasks
  val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
  // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
  val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
  // Regular expression for connecting to Spark deploy clusters
  val SPARK_REGEX = """spark://(.*)""".r
}

/**
 * A class encapsulating how to convert some type `T` from `Writable`. It stores both the `Writable`
 * class corresponding to `T` (e.g. `IntWritable` for `Int`) and a function for doing the
 * conversion.
 * The getter for the writable class takes a `ClassTag[T]` in case this is a generic object
 * that doesn't know the type of `T` when it is created. This sounds strange but is necessary to
 * support converting subclasses of `Writable` to themselves (`writableWritableConverter()`).
 */
private[spark] class WritableConverter[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: Writable => T)
  extends Serializable

object WritableConverter {

  // Helper objects for converting common types to Writable
  private[spark] def simpleWritableConverter[T, W <: Writable: ClassTag](convert: W => T)
  : WritableConverter[T] = {
    val wClass = classTag[W].runtimeClass.asInstanceOf[Class[W]]
    new WritableConverter[T](_ => wClass, x => convert(x.asInstanceOf[W]))
  }

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.

  // The following implicit declarations have been added on top of the very similar ones
  // below in order to enable compatibility with Scala 2.12. Scala 2.12 deprecates eta
  // expansion of zero-arg methods and thus won't match a no-arg method where it expects
  // an implicit that is a function of no args.

  implicit val intWritableConverterFn: () => WritableConverter[Int] =
    () => simpleWritableConverter[Int, IntWritable](_.get)

  implicit val longWritableConverterFn: () => WritableConverter[Long] =
    () => simpleWritableConverter[Long, LongWritable](_.get)

  implicit val doubleWritableConverterFn: () => WritableConverter[Double] =
    () => simpleWritableConverter[Double, DoubleWritable](_.get)

  implicit val floatWritableConverterFn: () => WritableConverter[Float] =
    () => simpleWritableConverter[Float, FloatWritable](_.get)

  implicit val booleanWritableConverterFn: () => WritableConverter[Boolean] =
    () => simpleWritableConverter[Boolean, BooleanWritable](_.get)

  implicit val bytesWritableConverterFn: () => WritableConverter[Array[Byte]] = {
    () => simpleWritableConverter[Array[Byte], BytesWritable] { bw =>
      // getBytes method returns array which is longer then data to be returned
      Arrays.copyOfRange(bw.getBytes, 0, bw.getLength)
    }
  }

  implicit val stringWritableConverterFn: () => WritableConverter[String] =
    () => simpleWritableConverter[String, Text](_.toString)

  implicit def writableWritableConverterFn[T <: Writable : ClassTag]: () => WritableConverter[T] =
    () => new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])

  // These implicits remain included for backwards-compatibility. They fulfill the
  // same role as those above.

  implicit def intWritableConverter(): WritableConverter[Int] =
    simpleWritableConverter[Int, IntWritable](_.get)

  implicit def longWritableConverter(): WritableConverter[Long] =
    simpleWritableConverter[Long, LongWritable](_.get)

  implicit def doubleWritableConverter(): WritableConverter[Double] =
    simpleWritableConverter[Double, DoubleWritable](_.get)

  implicit def floatWritableConverter(): WritableConverter[Float] =
    simpleWritableConverter[Float, FloatWritable](_.get)

  implicit def booleanWritableConverter(): WritableConverter[Boolean] =
    simpleWritableConverter[Boolean, BooleanWritable](_.get)

  implicit def bytesWritableConverter(): WritableConverter[Array[Byte]] = {
    simpleWritableConverter[Array[Byte], BytesWritable] { bw =>
      // getBytes method returns array which is longer then data to be returned
      Arrays.copyOfRange(bw.getBytes, 0, bw.getLength)
    }
  }

  implicit def stringWritableConverter(): WritableConverter[String] =
    simpleWritableConverter[String, Text](_.toString)

  implicit def writableWritableConverter[T <: Writable](): WritableConverter[T] =
    new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])
}

/**
 * A class encapsulating how to convert some type `T` to `Writable`. It stores both the `Writable`
 * class corresponding to `T` (e.g. `IntWritable` for `Int`) and a function for doing the
 * conversion.
 * The `Writable` class will be used in `SequenceFileRDDFunctions`.
 */
private[spark] class WritableFactory[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: T => Writable) extends Serializable

object WritableFactory {

  private[spark] def simpleWritableFactory[T: ClassTag, W <: Writable : ClassTag](convert: T => W)
    : WritableFactory[T] = {
    val writableClass = implicitly[ClassTag[W]].runtimeClass.asInstanceOf[Class[W]]
    new WritableFactory[T](_ => writableClass, convert)
  }

  implicit def intWritableFactory: WritableFactory[Int] =
    simpleWritableFactory(new IntWritable(_))

  implicit def longWritableFactory: WritableFactory[Long] =
    simpleWritableFactory(new LongWritable(_))

  implicit def floatWritableFactory: WritableFactory[Float] =
    simpleWritableFactory(new FloatWritable(_))

  implicit def doubleWritableFactory: WritableFactory[Double] =
    simpleWritableFactory(new DoubleWritable(_))

  implicit def booleanWritableFactory: WritableFactory[Boolean] =
    simpleWritableFactory(new BooleanWritable(_))

  implicit def bytesWritableFactory: WritableFactory[Array[Byte]] =
    simpleWritableFactory(new BytesWritable(_))

  implicit def stringWritableFactory: WritableFactory[String] =
    simpleWritableFactory(new Text(_))

  implicit def writableWritableFactory[T <: Writable: ClassTag]: WritableFactory[T] =
    simpleWritableFactory(w => w)

}
