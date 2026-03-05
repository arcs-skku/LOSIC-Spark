# LOSIC: Low-Overhead, Self-Improving Caching

This repository contains the artifact for **LOSIC**, a runtime cache planning technique for DAG-based distributed data analytics engines.

LOSIC computes a optimal caching plan under memory constraints and improves cumulative execution performance by reducing redundant recomputation and inefficient cache usage.

The implementation is based on **Apache Spark 2.4.7**.

---

# Repository Structure
```
LOSIC/
├── patch/
│ └── losic-spark-2.4.7.patch
├── conf/
│ └── spark-defaults.conf
├── scripts/
│ └── (example scripts)
└── README.md
```

- `patch/` : LOSIC modifications to the Spark engine  
- `conf/` : Spark configuration enabling LOSIC  
- `scripts/` : example scripts for running experiments  

<br>

# System Requirements

Example environment used in our experiments:

- Linux
- Java 8
- Scala 2.11
- Apache Spark 2.4.7
- Hadoop 2.7.x

<br>

# Setup

## 1. Download Apache Spark 2.4.7
```bash
wget https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7.tgz

tar -xzf spark-2.4.7.tgz
cd spark-2.4.7
```

<br>

## 2. Apply the LOSIC patch
```bash
patch -p1 < /path/to/LOSIC/patch/losic-spark-2.4.7.patch
```

<br>

## 3. Build Spark
```bash
build/mvn -Pyarn -Phadoop-2.7 -DskipTests clean package
```

<br>

## 4. Configure LOSIC

Copy the provided Spark configuration file:
```bash
cp /path/to/LOSIC/conf/spark-defaults.conf conf/
```

Alternatively, append the LOSIC-related parameters to your existing
`spark-defaults.conf`.

### Additional Spark Configuration

In addition to enabling LOSIC through `spark-defaults.conf`, the standard Spark cluster configuration is also required.

This includes configuring:

- `conf/master`
- `conf/slaves` (or `conf/workers` depending on the Spark distribution)
- Hadoop / HDFS paths if HDFS is used for storage
- Environment settings in `conf/spark-env.sh`

These configurations are identical to those required when setting up a **standard Apache Spark standalone cluster**.

Please refer to the official Spark documentation for detailed instructions on configuring a standalone cluster:

https://spark.apache.org/docs/2.4.7/spark-standalone.html

The LOSIC artifact does not modify the standard cluster setup procedure.

<br>

# Running LOSIC

Applications can be executed using the standard Spark submission interface.

Example:
```bash
bin/spark-submit
--class <main-class>
--master <spark-master>
<application-jar>
```

<br>

# LOSIC Configuration Parameters

LOSIC provides several configuration parameters to control profiling, caching optimization, and debugging behaviors.  
The following parameters can be specified in `spark-defaults.conf`.

## Essential Parameters

| Parameter | Default | Description |
|---|---|---|
| `spark.ss.profile.enabled` | `false` | Enables runtime profiling. In typical LOSIC usage this should be set to `true`. It can be disabled to avoid initial profiling overhead for workloads with little or no reuse. |
| `spark.ss.profile.max` | `1000` | Maximum number of profiling samples per operator. Empirically, `1000` provides a good balance between accuracy and overhead. |
| `spark.ss.profile.threshold` | `0.99` | Threshold used by the profiling planner to tolerate estimation errors when deciding whether to profile the next operation. In practice, values around `0.95–0.99` work well. |
| `spark.ss.optimize.enabled` | `false` | Enables LOSIC cache optimization. When enabled, LOSIC determines cache placement based on collected profiling data. If insufficient profiling data exists, the optimization module may be skipped. |
| `spark.ss.unpersist.enabled` | `false` | Enables proactive unpersist operations triggered by LOSIC’s cache decisions when memory pressure occurs. Typically left `false`, but it may be automatically enabled when optimization is active and memory becomes constrained. |

## Experimental or Debugging Parameters

| Parameter | Default | Description |
|---|---|---|
| `spark.ss.optimize.mode` | `overlap` | Determines when cache optimization is performed. `overlap` runs optimization concurrently with stage execution, while `ahead` performs optimization before stage execution begins. `overlap` is recommended to minimize overhead. |
| `spark.ss.mature.enabled` | `false` | Precomputes caching benefits for all candidates at application startup instead of computing them per stage. This introduces large startup overhead and is mainly intended for experimentation. |
| `spark.ss.cache.plan` | `refreshing` | Strategy for updating cache decisions. `refreshing` recomputes decisions each stage, `enhancing` incrementally adds new candidates, and `pruning` initially considers all candidates and removes low-benefit ones. `refreshing` generally provides the best balance between performance and memory pressure. |
| `spark.ss.unpersist.forceDisabled` | `false` | Forces unpersist operations to remain disabled even when memory pressure occurs. |
| `spark.ss.memCheck.enabled` | `false` | Enables memory capacity checks at each stage. Useful for debugging memory behavior. |
| `spark.ss.countFunctional.enabled` | `false` | Counts functional operators in each stage. Intended for internal analysis and debugging. |
| `spark.ss.log.enabled` | `false` | Enables detailed debugging logs for LOSIC internal components. |
| `spark.ss.noCache.enabled` | `false` | Disables all caching operations. Useful for baseline comparisons. |
| `spark.ss.prepare.enabled` | `false` | Enables collection of RDD size information for analysis and debugging. |

### Example Configuration

Example configuration enabling LOSIC:

```bash
spark.ss.profile.enabled        true
spark.ss.profile.max            1000
spark.ss.profile.threshold      0.95
spark.ss.optimize.enabled       true
spark.ss.unpersist.enabled      false
```
