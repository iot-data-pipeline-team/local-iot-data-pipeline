# Cloud IoT Data Pipeline

## Overview

This project extends the **Real-Time Industrial IoT Data Pipeline** from a local Docker-based environment to a simple AWS cloud architecture.

The pipeline simulates industrial machines and processes their sensor data using:

* **EC2** for the IoT event producer and Kafka
* **Amazon EMR** for Spark Structured Streaming
* **Amazon S3** for cloud storage and Spark checkpoints
* **VPC** for secure networking between the components
* **S3 VPC Gateway Endpoint** for private communication between EMR and S3

The goal of this phase is to understand how the existing local pipeline can be moved to AWS while keeping the architecture simple and avoiding managed services such as Amazon MSK and AWS Glue initially.

---

# Architecture

```text
                         AWS Region
                              │
                              ▼
                    ┌───────────────────┐
                    │       VPC         │
                    │    10.0.0.0/16    │
                    │                   │
                    │  ┌─────────────┐  │
                    │  │   Public    │  │
                    │  │   Subnet    │  │
                    │  │             │  │
                    │  │    EC2      │  │
                    │  │   ├─Producer │  │
                    │  │   └─Kafka   │  │
                    │  └──────┬──────┘  │
                    │         │         │
                    │    Private IP      │
                    │         │         │
                    │  ┌──────▼──────┐   │
                    │  │   Private   │   │
                    │  │   Subnet    │   │
                    │  │             │   │
                    │  │    EMR      │   │
                    │  │   └─Spark   │   │
                    │  └──────┬──────┘   │
                    │         │          │
                    │   S3 VPC Endpoint  │
                    └─────────┼──────────┘
                              │
                              ▼
                             S3
```

---

# Data Flow

The data flows through the pipeline as follows:

```text
IoT Producer
     │
     ▼
Kafka on EC2
     │
     │ Private VPC communication
     ▼
Spark Structured Streaming on EMR
     │
     ├── Processing
     ├── Validation
     ├── Transformation
     ├── Enrichment
     └── Aggregation
     │
     ▼
Amazon S3
```

---

# AWS Components

## 1. Amazon VPC

The pipeline is deployed inside a dedicated VPC.

### VPC CIDR

```text
10.0.0.0/16
```

The VPC provides the private networking environment for EC2 and EMR.

The architecture uses two types of subnets:

```text
Public Subnet
    │
    └── EC2

Private Subnet
    │
    └── EMR
```

---

## 2. Public Subnet

The public subnet contains the EC2 instance.

```text
Public Subnet
│
└── EC2
    ├── IoT Producer
    └── Kafka
```

The EC2 instance is placed in the public subnet because it needs controlled external access for administration and development.

The Kafka service itself should **not be unnecessarily exposed to the public internet**.

---

## 3. EC2

The EC2 instance hosts the components that generate and ingest the IoT events.

```text
EC2
├── Producer
└── Kafka
```

### Producer

The producer simulates industrial IoT devices and generates sensor events such as:

* Machine ID
* Timestamp
* Temperature
* Pressure
* Vibration
* Load
* Machine status
* Fault/anomaly information

The producer publishes events to Kafka topics.

### Kafka

Kafka acts as the real-time event streaming platform.

Example:

```text
Producer
   │
   ▼
Kafka Topic
   │
   ▼
Spark
```

Kafka remains self-managed on EC2 in this architecture.

Amazon MSK is intentionally not introduced at this stage.

---

# 4. Private Subnet

The private subnet contains the EMR cluster.

```text
Private Subnet
│
└── EMR
    └── Spark
```

The EMR cluster does not need to be directly accessible from the public internet.

Spark communicates with Kafka using the **private IP address of the EC2 instance**.

Example:

```text
EC2 Private IP
10.0.1.x:9092
        │
        │ VPC private network
        ▼
EMR Spark
```

This communication does not require a NAT Gateway.

---

# 5. Amazon EMR

Amazon EMR is used to run the Spark processing layer.

The Spark application performs the same major processing stages as the local pipeline:

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ├── Bronze
  ├── Silver
  └── Gold
```

### Spark responsibilities

The Spark jobs are responsible for:

* Reading real-time Kafka events
* Parsing JSON messages
* Data validation
* Data cleaning
* Data transformation
* Data enrichment
* Business-rule processing
* Streaming aggregations
* Writing processed data to S3

The objective is to move the compute layer from local Spark to a managed EMR cluster without changing the core data-processing logic.

---

# 6. Amazon S3

Amazon S3 is used as the cloud storage layer.

The processed data can be organized using a Medallion Architecture:

```text
S3
│
├── bronze/
│
├── silver/
│
└── gold/
```

### Bronze

Contains raw or minimally transformed streaming data.

```text
s3://<bucket>/bronze/
```

### Silver

Contains validated, cleaned, and enriched data.

```text
s3://<bucket>/silver/
```

### Gold

Contains business-level aggregations and analytical datasets.

```text
s3://<bucket>/gold/
```

---

# 7. S3 VPC Gateway Endpoint

The EMR cluster is located in a private subnet, while S3 is an AWS-managed service outside the VPC.

To allow EMR to access S3 without requiring a NAT Gateway, the VPC uses an **S3 Gateway VPC Endpoint**.

The communication path is:

```text
EMR
 │
 ▼
Private Route Table
 │
 ▼
S3 Gateway VPC Endpoint
 │
 ▼
Amazon S3
```

This allows the private EMR resources to access S3 without requiring direct internet access.

The endpoint is associated with the appropriate route table used by the private subnet.

---

# 8. EC2 → EMR Communication

EC2 and EMR communicate through the VPC's private network.

```text
EC2
Private IP
    │
    │
    │ VPC local routing
    ▼
EMR
Private IP
```

Spark uses the EC2 private IP address when connecting to Kafka.

For example:

```text
<EC2_PRIVATE_IP>:9092
```

The connection is controlled using AWS Security Groups.

The Kafka port should allow traffic from the appropriate EMR security group rather than from the entire internet.

---

# 9. EMR → S3 Communication

The Spark application needs S3 for:

* Bronze data
* Silver data
* Gold data
* Streaming checkpoints
* Potential application artifacts and configuration

The communication path is:

```text
Spark on EMR
      │
      ▼
Private Subnet
      │
      ▼
S3 VPC Gateway Endpoint
      │
      ▼
Amazon S3
```

No NAT Gateway is required specifically for this EMR-to-S3 communication.

---

# Networking Design

## VPC

```text
CIDR: 10.0.0.0/16
```

## Subnets

Initial design:

```text
Public Subnet
    └── EC2

Private Subnet
    └── EMR
```

The architecture can later be expanded to multiple Availability Zones for higher availability.

---

# Route Tables

The public subnet uses a route table that provides internet connectivity through an Internet Gateway.

Conceptually:

```text
Public Route Table

Destination       Target
10.0.0.0/16       local
0.0.0.0/0         Internet Gateway
```

The private subnet uses a route table that provides local VPC communication and a route to the S3 VPC endpoint.

Conceptually:

```text
Private Route Table

Destination       Target
10.0.0.0/16       local
S3 Prefix List    S3 Gateway Endpoint
```

---

# Security Groups

Security groups control communication between EC2 and EMR.

The main requirement is:

```text
EMR Security Group
        │
        ▼
EC2 Security Group
        │
        ▼
Kafka Port
```

Kafka should allow connections from the EMR security group.

Administrative access such as SSH should be restricted to the developer's trusted IP rather than exposed unnecessarily.

---

# Why No NAT Gateway?

The initial architecture intentionally avoids a NAT Gateway.

The EMR cluster needs to communicate with:

```text
EC2 → Kafka
```

and:

```text
EMR → S3
```

The first communication uses the VPC's private network, while the second uses the S3 Gateway VPC Endpoint.

Therefore, a NAT Gateway is not required for these two communication paths.

This also keeps the initial learning environment simpler and avoids unnecessary NAT Gateway charges.

If future EMR requirements need general outbound internet access, NAT Gateway or another controlled egress solution can be evaluated.

---

# Streaming Fault Tolerance

The pipeline uses two different mechanisms for handling failures.

## EMR Worker Failure

If a Spark worker node fails, Spark can retry failed tasks and recompute required work on another available worker.

Conceptually:

```text
Worker 1
   │
   └── Task ❌
          │
          ▼
    Spark Scheduler
          │
          ▼
       Worker 2
          │
          └── Task retried
```

EMR can also manage failed cluster nodes depending on the cluster configuration.

---

## Spark Application Failure

If the Spark Structured Streaming application itself stops, checkpointing allows the application to recover its streaming progress and state.

Conceptually:

```text
Kafka
  │
  ▼
Spark
  │
  ▼
Checkpoint
  │
  ▼
S3
```

After a failure:

```text
Spark Application
       │
       X
    Failure
       │
       ▼
    Restart
       │
       ▼
Read checkpoint
       │
       ▼
Resume processing
```

For this reason, streaming checkpoints should be stored in durable storage rather than only on the local filesystem of an EMR node.

---

# Fault Tolerance vs Disaster Recovery

These concepts are related but different.

### Fault tolerance

Handles component failures while the system is running.

Examples:

* Spark task retry
* EMR worker replacement
* Kafka replication
* Streaming checkpoint recovery

### Disaster recovery

Handles larger failures or situations where the system needs to be restored.

Examples:

* Data backups
* S3 versioning
* Multi-AZ architecture
* Cross-region recovery
* Infrastructure recreation

The initial project focuses primarily on **fault tolerance and basic recovery**.

---

# Current Architecture Scope

The first cloud version intentionally uses a limited number of AWS services:

```text
AWS
│
├── VPC
│
├── EC2
│   ├── Producer
│   └── Kafka
│
├── EMR
│   └── Spark
│
├── S3
│
└── S3 VPC Gateway Endpoint
```

We intentionally do **not** introduce the following services yet:

* Amazon MSK
* AWS Glue
* AWS Glue Data Catalog
* Amazon Redshift
* Amazon RDS
* Kinesis
* Multi-region architecture

The objective is to first understand the fundamental cloud architecture and networking.

---

# Local vs Cloud Architecture

## Local

```text
Producer
    │
    ▼
Docker Kafka
    │
    ▼
Local Spark
    │
    ▼
Local Storage
```

## Cloud

```text
EC2
├── Producer
└── Kafka
     │
     │ Private VPC Network
     ▼
EMR
└── Spark
     │
     │ S3 VPC Endpoint
     ▼
S3
```

The main architectural change is that the **event ingestion layer runs on EC2**, while the **distributed processing layer runs on EMR**, with S3 becoming the durable cloud storage layer.

---

# Deployment Plan

The cloud migration will be performed incrementally.

### Phase 1 — Networking

* Create the AWS VPC
* Configure the VPC CIDR
* Create public and private subnets
* Create route tables
* Attach an Internet Gateway
* Create the S3 Gateway VPC Endpoint
* Configure Security Groups

### Phase 2 — EC2 and Kafka

* Launch EC2
* Install Docker
* Deploy Kafka
* Deploy the IoT producer
* Configure Kafka networking
* Test producer → Kafka

### Phase 3 — EMR

* Create the EMR cluster
* Place EMR in the private subnet
* Configure EMR security groups
* Configure Spark
* Test EMR → Kafka connectivity

### Phase 4 — S3

* Create the S3 bucket
* Configure the required S3 access
* Configure the S3 VPC Endpoint
* Configure Spark output paths
* Configure Spark checkpoints

### Phase 5 — End-to-End Pipeline

```text
Producer
    │
    ▼
Kafka on EC2
    │
    ▼
Spark on EMR
    │
    ▼
S3
```

Validate the complete streaming pipeline from event generation to cloud storage.

---

# Future Improvements

After the basic architecture is working, the project can be extended with:

* Kafka replication across multiple brokers
* Multiple Availability Zones
* EMR fault-tolerant cluster configuration
* Spark Structured Streaming watermarks
* Windowed aggregations
* Late-event handling
* S3 lifecycle policies
* S3 data partitioning
* AWS Glue Data Catalog
* AWS Glue ETL
* Amazon Athena
* Amazon Redshift
* Managed Kafka using Amazon MSK
* Apache Airflow for orchestration
* Monitoring and alerting
* Infrastructure as Code using Terraform or AWS CloudFormation
* Multi-region disaster recovery

---

# Final Architecture

The target architecture for this phase is:

```text
                         AWS Region
                              │
                    ┌─────────▼─────────┐
                    │        VPC        │
                    │    10.0.0.0/16    │
                    │                   │
                    │ ┌───────────────┐ │
                    │ │ Public Subnet │ │
                    │ │               │ │
                    │ │     EC2       │ │
                    │ │  ┌─────────┐  │ │
                    │ │  │Producer │  │ │
                    │ │  │ Kafka   │  │ │
                    │ │  └────┬────┘  │ │
                    │ └───────┼───────┘ │
                    │         │         │
                    │    Private IP     │
                    │         │         │
                    │ ┌───────▼───────┐ │
                    │ │Private Subnet │ │
                    │ │               │ │
                    │ │      EMR      │ │
                    │ │      │        │ │
                    │ │    Spark      │ │
                    │ └───────┬───────┘ │
                    │         │         │
                    │   S3 VPC Endpoint │
                    └─────────┼─────────┘
                              │
                              ▼
                             S3
```

The core principle is:

**EC2 handles event generation and Kafka ingestion → EMR handles distributed Spark processing → S3 provides durable cloud storage, while VPC networking keeps communication between the components controlled and private where appropriate.**
