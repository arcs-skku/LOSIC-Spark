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



# System Requirements

Example environment used in our experiments:

- Linux
- Java 8
- Scala 2.11
- Apache Spark 2.4.7
- Hadoop 2.7.x



# Setup

## 1. Download Apache Spark 2.4.7
```bash
wget https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7.tgz

tar -xzf spark-2.4.7.tgz
cd spark-2.4.7
```



## 2. Apply the LOSIC patch
```bash
patch -p1 < /path/to/LOSIC/patch/losic-spark-2.4.7.patch
```



## 3. Build Spark
```bash
build/mvn -Pyarn -Phadoop-2.7 -DskipTests clean package
```



## 4. Configure LOSIC

Copy the provided Spark configuration file:
```bash
cp /path/to/LOSIC/conf/spark-defaults.conf conf/
```

Alternatively, append the LOSIC-related parameters to your existing
`spark-defaults.conf`.



# Running LOSIC

Applications can be executed using the standard Spark submission interface.

Example:
```bash
bin/spark-submit
--class <main-class>
--master <spark-master>
<application-jar>
```
