# AWS EMR

This lesson covers the README's "AWS EMR" section (lines 759-929). The hands-on build lives in `5.emr-code/`. Note: the README's setup text mentions a `4.emr-code` directory. That directory does not exist in this repo. The correct folder is `5.emr-code`.

## What EMR Is

EMR is a managed cluster platform. It runs big data frameworks for you. You do not install Hadoop or Spark by hand. EMR installs and configures them on a cluster of EC2 instances, on EKS, or in a serverless mode.

## EMR on EC2: Cluster Architecture

An EMR-on-EC2 cluster can have up to three node types. Each node type has a distinct job.

| Node type | Runs tasks | Stores HDFS data | Required? |
|---|---|---|---|
| Master (primary) | Coordinates the cluster | No | Yes, always |
| Core | Yes | Yes | No, but needed for any multi-node cluster |
| Task | Yes | No | No, fully optional |

### Master node

The master node manages the cluster. It runs the YARN ResourceManager and the HDFS NameNode. It tracks the health of other nodes. It distributes work to core and task nodes. It does not usually run the actual data processing tasks itself.

### Core node

A core node runs tasks and stores data in HDFS. The HDFS DataNode process runs on every core node. A multi-node cluster needs at least one core node. Core nodes are the only place cluster data physically lives, aside from data you keep in S3.

### Task node

A task node runs tasks only. It does not store HDFS data. Task nodes are optional. You add them purely for extra compute power when a job needs more parallelism than the core nodes provide.

### Why only the master node is strictly required

A one-node EMR cluster is possible. In that case, the single node acts as master, core, and task at once. Core and task nodes exist to scale out compute and storage beyond what one node offers. They are not required for the cluster to function.

### HDFS and why core nodes store the data

HDFS (Hadoop Distributed File System) splits files into blocks and spreads the blocks across nodes. It replicates each block so that a node failure does not lose data. EMR sets the default replication factor based on cluster size:

- 1 to 3 core+task nodes: replication factor 1 (no redundancy)
- 4 to 9 core+task nodes: replication factor 2
- 10 or more core+task nodes: replication factor 3

Only core nodes run the HDFS DataNode process. Task nodes deliberately do not, because EMR expects task nodes to scale in and out often. Storing HDFS blocks on a node that can disappear at any time would risk data loss and force constant re-replication. Keeping storage on core nodes only, and compute-only capacity on task nodes, cleanly separates "durable state" from "elastic capacity."

### Losing a task node vs. losing a core node

This is a common exam distinction.

**Lose a task node:** Any YARN containers (task attempts) running on it fail. YARN's ApplicationMaster reschedules that work on other available nodes. No data is lost, because no HDFS blocks lived there. This is why task nodes are the standard target for Spot Instances: an interruption only costs you re-computation, never data.

**Lose a core node:** You lose both compute (its running tasks fail and are rescheduled, same as a task node) and one replica of every HDFS block stored on it. If the replication factor is 2 or 3, the data usually still exists on other core nodes, but redundancy drops and HDFS must re-replicate the affected blocks onto remaining nodes to restore the target factor. If you lose enough core nodes at once, or the cluster's replication factor is 1, you can lose data outright. Because of this, EMR treats core node removal more carefully than task node removal, and shrinking the core group is slower and riskier than shrinking the task group. Practical rule: put Spot Instances on task nodes for cost savings; keep core nodes on On-Demand (or use Spot there cautiously) because losing them threatens data, not just compute.

## Transient vs. Persistent Clusters

| | Transient cluster | Persistent (long-running) cluster |
|---|---|---|
| Lifecycle | Created for one job, auto-terminates when steps finish | Stays running for days, weeks, or months |
| Billing | Pay only for the run duration | Pay continuously, including idle time |
| Best fit | Scheduled batch ETL (nightly jobs, one-off transforms) | Interactive/ad hoc work: notebooks, Hive/Presto queries, shared data science use |
| Typical trigger | Orchestrated by Step Functions, a scheduler, or a pipeline | Created once via console/CLI and left running |

The tradeoff is straightforward. Transient clusters minimize cost because you never pay for idle capacity, but they add startup latency to every job (a few minutes to provision). Persistent clusters remove that startup latency and suit unpredictable, ad hoc query patterns, but you pay for every idle hour. EMR exposes an `IsIdle` CloudWatch metric specifically so you can detect an idle persistent cluster and decide whether to shut it down.

## Processing Frameworks and YARN

EMR's core frameworks are **Hadoop MapReduce** and **Apache Spark**. These are the two general-purpose distributed processing engines the exam focuses on most.

EMR also supports several other frameworks, which the tutorial setup in this repo installs: **Hive** (SQL over Hadoop/Spark), **Pig** (a scripting language for data flows, largely legacy), **Presto** and **Trino** (fast distributed SQL query engines), **HBase** (a NoSQL wide-column store on top of HDFS), and **Flink** (stream processing).

**YARN (Yet Another Resource Negotiator)** is the default cluster resource manager. Its job: schedule work and negotiate compute resources (memory, vCPU) among frameworks that share the same cluster. When Spark and Hive both run on one cluster, YARN is what decides how much of the cluster each gets at a given moment, and queues work when demand exceeds capacity.

Exam-relevant nuance: not every framework in the list above runs on YARN. Spark, Hadoop MapReduce, Hive, and Flink are YARN-based. **Presto and HBase are not** — they manage their own resources outside YARN. This matters directly for autoscaling (see below): EMR's automatic scaling reacts to YARN metrics, so it does not see load generated by Presto or HBase.

## Storage: HDFS vs. EMRFS

**HDFS** is local, cluster-attached storage spread across core nodes, as described above. It is fast for local reads/writes and is the natural place for **intermediate shuffle data** during a Spark or MapReduce job — the temporary data exchanged between processing stages. HDFS data lives and dies with the cluster; terminate the cluster and, unless you copied the data elsewhere, it is gone.

**EMRFS** is EMR's connector that lets Hadoop-compatible applications read and write **Amazon S3** as if it were a filesystem. This is what most AWS-native EMR usage relies on, for one core reason: it **decouples storage from compute**. Your data lives durably in S3, independent of any cluster's lifecycle. You can spin up a transient cluster, point it at S3 via EMRFS, run the job, terminate the cluster, and the data is untouched. This is what makes the transient-cluster cost model (above) actually workable — if data lived only in HDFS, you could never safely terminate a cluster.

**When to still reach for HDFS:** performance-sensitive intermediate data within a single job run (shuffle, spill, checkpoints), and any workload where you need fast local disk I/O rather than network calls to S3. In practice, a well-designed EMR job reads raw input from S3 (EMRFS), uses HDFS for its own intermediate/shuffle data during processing, and writes final output back to S3 (EMRFS).

Historical footnote, not usually exam-critical today: EMRFS once needed an optional "consistent view" feature (backed by DynamoDB) to work around S3's old eventual-consistency behavior. S3 has been strongly consistent since December 2020, so this workaround is largely legacy.

## EMR Serverless

EMR Serverless removes cluster and instance management entirely. Operationally, this is the key difference from EMR on EC2:

| | EMR on EC2 | EMR Serverless |
|---|---|---|
| You provision | Master/core/task EC2 instances | Nothing — no instances, no cluster sizing |
| You choose | Instance types, node counts, scaling policy | An EMR release version + a runtime type |
| Billing | Per instance-hour, cluster running or idle | Per vCPU/memory-second actually consumed by a job |

You pick a release version and a runtime environment. **Correction to the README:** the README states the runtime choices are "Hive, Spark, Presto." As of current AWS documentation, EMR Serverless supports only two application types: **Spark and Hive**. Presto/Trino is not an EMR Serverless application type — it runs on EMR on EC2 or, separately, as Amazon Athena. Do not repeat "Presto" as an EMR Serverless option on the exam.

### Application lifecycle states (verified against AWS docs)

An EMR Serverless application moves through these states:

- **CREATING** — application is being set up, not yet usable
- **CREATED** — exists, but has not provisioned any capacity yet
- **STARTING** — provisioning capacity
- **STARTED** — ready to accept job runs (jobs only get accepted in this state)
- **STOPPING** — jobs have finished, capacity is being released
- **STOPPED** — no resources running; can be restarted or reconfigured
- **TERMINATED** — permanently deleted, no longer listed

The README's diagram is consistent with this list; it just does not name every intermediate state. The important exam point: **you are billed for the resources an application holds while STARTED (and while jobs run on it)**. Because an application can sit idle in the STARTED state without auto-stopping (unless you configure auto-stop), you must explicitly stop or terminate it to guarantee billing stops — the same idle-cost trap as a persistent EMR-on-EC2 cluster, just at the application level instead of the cluster level.

## EMR on EKS

EMR on EKS runs EMR workloads on an **existing Amazon EKS (Kubernetes) cluster**, instead of EMR provisioning and managing its own EC2 instances. You register a "virtual cluster" that maps to a Kubernetes namespace, and job runs execute as pods inside that namespace. Compute can be backed by EC2 or Fargate.

Unlike EMR on EC2, which runs many frameworks side by side, EMR on EKS is primarily built around **Apache Spark** (jobs submitted via the EMR API, `spark-submit`, or the Spark Operator).

**When EMR on EKS makes sense:** your organization has already standardized on Kubernetes/EKS as its compute platform for other workloads, and you want big data jobs to share that same cluster capacity, namespace isolation, and operational tooling (monitoring, RBAC, CI/CD) rather than running a separate EMR-managed fleet. It is a good fit when platform teams want one shared Kubernetes control plane across many teams and workload types (web services, batch jobs, ML, Spark), instead of maintaining EMR-on-EC2 clusters as a separate operational silo.

**When it does not make sense:** if you have no existing EKS investment, plain EMR on EC2 or EMR Serverless is simpler — you are not gaining anything by adding Kubernetes as a dependency.

## Three-Way Comparison: EMR on EC2 vs. EMR Serverless vs. EMR on EKS

| | EMR on EC2 | EMR Serverless | EMR on EKS |
|---|---|---|---|
| Who manages infrastructure | You (instance types, cluster sizing, scaling policy) | AWS (no instances or cluster to manage) | You/your platform team manage the EKS cluster; EMR manages the job containers within it |
| Billing granularity | Per EC2 instance-hour, whole cluster (running or idle) | Per vCPU/memory-second actually used by jobs | Per underlying EKS compute (EC2 or Fargate) actually used by job pods |
| Typical use case | Full framework flexibility (Hadoop, Spark, Hive, Presto, HBase, Flink); fine-grained cluster tuning | Spark/Hive jobs where you don't want to think about cluster sizing at all | Spark workloads on a shared, already-existing Kubernetes platform |
| Operational control | Highest — you tune instance types, node counts, bootstrap actions, scaling rules | Lowest — AWS handles provisioning and scaling within your resource limits | High for Kubernetes-native teams — Kubernetes-style operations (namespaces, RBAC), but Spark-focused |

Exam framing: this is a "who manages what, and what do you pay for" question. EC2 = most control, most operational responsibility. Serverless = least operational overhead, pay-per-use, narrower framework choice (Spark/Hive only). EKS = you bring your own cluster (Kubernetes), EMR brings the big-data job execution on top of it.

## Security

At a conceptual level, for the exam:

- **Encryption at rest and in transit** is supported for EMR, covering both data on cluster storage (HDFS, local disks) and data moving between nodes or to/from S3.
- **AWS Lake Formation** and **Apache Ranger** both provide fine-grained access control (column/row/table-level permissions) layered on top of plain IAM, which alone only controls access at the resource level (e.g., "can call this API," "can read this S3 bucket"), not inside a dataset.
- **Network security** is enforced the usual VPC way: security groups control inbound/outbound traffic at the instance level, and NACLs control traffic at the subnet level.

This lesson treats security only at this conceptual level, matching how briefly the README covers it. Do not treat this as a full security design — verify actual controls with a security owner before applying them to a real workload.

## Orchestration and Automation

### EMR Steps

A **step** is a unit of work submitted to a running cluster (or to a transient cluster at creation time) — for example, "run this Spark job." The tutorial adds a step using **`command-runner.jar`**, a generic wrapper that lets a step run an arbitrary shell command (here, `spark-submit ...`) instead of requiring a purpose-built step type for every tool. This is how you turn "SSH in and run spark-submit by hand" into something you can automate and re-run without a person present.

### Step Functions for EMR orchestration

The tutorial uses **AWS Step Functions** to create the cluster, submit steps, and terminate the cluster, driven by a JSON state machine definition. Step Functions is a natural fit here for the same reasons covered in the Step Functions vs. EventBridge vs. Glue Workflows vs. MWAA comparison from the Glue lesson, applied to EMR instead of Glue:

- Step Functions has direct, built-in service integrations for the EMR API (create cluster, add steps, terminate cluster), including a "run and wait for completion" pattern, with native retry/catch error handling — no custom glue code needed.
- EventBridge is event-driven/reactive (react to a state change), not a sequence orchestrator — it is a poor fit for "do A, then B, then C, then clean up."
- Glue Workflows are scoped to Glue's own components (crawlers, Glue jobs, triggers) — not EMR.
- MWAA (managed Airflow) fits complex, code-heavy, cross-system DAGs with rich branching logic, but carries a bigger operational footprint (its own environment to run) than a serverless Step Functions state machine needs for a straightforward create-cluster-run-steps-terminate sequence.

For a scheduled, transient EMR job, Step Functions gives you the orchestration with the least operational overhead.

### Autoscaling — correction on metric naming

The tutorial's autoscaling walkthrough sets a **custom scaling policy** using a metric called `AppsRunning`, with a threshold (`>= 2` to scale out, `< 2` to scale in), an evaluation period (one five-minute period), and a cooldown (60 seconds). I verified this against current AWS documentation, and it needs one clarification, not a correction of fact:

**`AppsRunning` is a real, valid EMR CloudWatch metric** (namespace `AWS/ElasticMapReduce`) — it counts YARN applications currently running. So the README's tutorial is technically accurate, not fabricated.

However, EMR actually has **two distinct scaling features**, and the exam is more likely to test the newer one:

1. **Automatic scaling with a custom policy** (the older feature, and what this tutorial uses) — you manually pick a CloudWatch metric (`AppsRunning`, `ContainerPendingRatio`, `YARNMemoryAvailablePercentage`, etc.), a comparison operator, a threshold, an evaluation period, and a cooldown, for scale-out and scale-in separately. Only works with instance groups, not instance fleets.
2. **EMR Managed Scaling** (the newer, AWS-recommended feature) — you set only a minimum and maximum cluster capacity. EMR's own internal algorithm decides when and how much to scale, using metrics like `YARNMemoryAvailablePercentage`, `ContainerPendingRatio`, and `HDFSUtilization` under the hood — you never pick a metric or threshold yourself. Works with both instance groups and instance fleets.

The README's tutorial demonstrates option 1. Current AWS guidance and most current exam material lean toward option 2 (Managed Scaling) as the recommended, lower-effort approach. Know both exist, know Managed Scaling is the "just set min/max and let EMR handle it" answer, and know that autoscaling driven by YARN metrics only reacts to YARN-based frameworks (Spark, Hadoop, Hive, Flink) — not Presto or HBase, which don't run on YARN.

## AWS Glue Data Catalog as EMR's Metastore

The README notes that EMR can use the **AWS Glue Data Catalog** as its Hive-compatible metastore (the tutorial setup explicitly selects "Use for Hive table metadata" and "Use for Spark table metadata" when creating the cluster). This is the same Glue Data Catalog covered in your Glue lesson: one central, serverless, managed metastore that Hive, Spark, Presto/Trino, Athena, and Glue jobs can all share, instead of each engine keeping its own separate metastore. Using it with EMR means table definitions created by a Glue job or crawler are immediately visible to Hive/Spark on your EMR cluster, and vice versa, without any manual syncing.

## Other README Simplifications Worth Flagging

- The setup instructions reference a `4.emr-code` directory that does not exist in this repo (the real folder is `5.emr-code`). This is a repo navigation issue, not an EMR concept issue, but it will mislead you if you go looking for the CloudFormation script by that name.
- The README's one-line definitions of master/core/task nodes are correct but minimal — they do not explain the consequence of losing each node type, which is exactly the kind of "what happens if" reasoning the exam tests. See the dedicated subsection above.
- The README states EMR Serverless's runtime choices as "Hive, Spark, Presto" — Presto is not a supported EMR Serverless application type. Corrected above.
- The README's autoscaling section teaches the older custom-policy mechanism without mentioning EMR Managed Scaling exists as a separate, newer, generally-preferred feature. Both are covered above.

## Exam Checklist

- Master node: always required, coordinates the cluster, runs ResourceManager + NameNode. Core node: runs tasks and stores HDFS data (DataNode); at least one needed for a multi-node cluster. Task node: runs tasks only, stores no data, fully optional.
- Losing a task node costs you re-computation only. Losing a core node costs you re-computation plus a replica of every HDFS block it held — put Spot Instances on task nodes, not core nodes.
- HDFS default replication: 1 (< 4 core+task nodes), 2 (4-9 nodes), 3 (10+ nodes).
- Transient cluster = pay only for job duration, best for scheduled batch ETL. Persistent cluster = always-on, best for interactive/ad hoc work, but you pay for idle time (watch the `IsIdle` metric).
- YARN is EMR's default resource manager, scheduling and sharing resources across frameworks on one cluster. Spark, Hadoop MapReduce, Hive, and Flink run on YARN. Presto and HBase do not.
- EMRFS (S3 as a filesystem) decouples storage from compute and is the standard AWS-native choice — data survives cluster termination. HDFS is still useful for fast local/intermediate shuffle data during a job.
- EMR Serverless: no instances to size, choose a release version + runtime (Spark or Hive only — not Presto), billed per resource-second used. Application states: CREATING → CREATED → STARTING → STARTED → STOPPING → STOPPED, or TERMINATED. Billing only truly stops once you stop/terminate — an idle STARTED application still costs money.
- EMR on EKS: runs (mainly Spark) EMR workloads on an existing EKS cluster via a virtual cluster mapped to a namespace. Best when you're already standardized on Kubernetes and want to share cluster capacity across teams/workloads.
- Three-way comparison: EC2 = most control/most management; Serverless = least management, pay-per-use, narrowest framework choice; EKS = you own the Kubernetes layer, EMR runs jobs on top.
- Fine-grained access control on EMR data comes from Lake Formation or Apache Ranger, layered on top of IAM (IAM alone is resource-level, not row/column-level).
- EMR Steps + `command-runner.jar` let you automate arbitrary shell commands (like `spark-submit`) as a trackable, re-runnable unit of work.
- Step Functions is the natural orchestrator for create-cluster → submit-steps → terminate-cluster because of its native EMR API integrations and built-in retry/wait semantics — same logic as the Glue orchestration comparison, applied to EMR.
- Two separate EMR scaling features exist: custom scaling policy (you pick the metric, e.g. `AppsRunning`, `ContainerPendingRatio`, `YARNMemoryAvailablePercentage`) vs. EMR Managed Scaling (you set min/max only, EMR decides internally). Expect the exam to favor Managed Scaling as the recommended approach.
- The Glue Data Catalog can serve as EMR's Hive-compatible metastore, shared across Hive, Spark, Presto/Trino, Athena, and Glue jobs.
