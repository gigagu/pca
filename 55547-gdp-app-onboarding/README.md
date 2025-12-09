# 55547-gdp-app-onboarding

## Overview

**55547-gdp-app-onboarding** is an automated onboarding system for multi-tenant big data platforms. It streamlines the process of tenant registration, resource provisioning, and permission configuration across Kubernetes, MinIO, Trino, Ranger, and other components. The project is designed to help both platform teams and tenant owners efficiently manage and operate their data workloads in a secure, scalable, and cloud-native environment.

---

## Features

- **Automated Tenant Onboarding:**
  Parses tenant manifests and provisions all required resources automatically.
- **Kubernetes Native:**
  Creates namespaces, resource quotas, and YuniKorn queues for each tenant.
- **Object Storage Integration:**
  Registers MinIO buckets for tenant data storage.
- **Compute Resource Management:**
  Configures Trino resource groups and selectors for tenant compute isolation.
- **Data Governance:**
  Applies Ranger policies for fine-grained data access control.
- **Extensible Workflow:**
  Supports integration with Airflow, DataHub, Kafka, and more.
- **Concurrent Processing:**
  Handles multiple tenants in parallel with robust error aggregation.

---

## Quick Start

### Prerequisites

- Go 1.18+
- Kubernetes cluster
- MinIO, Trino, Ranger, YuniKorn deployed
- [kubebuilder](https://book.kubebuilder.io/)

### Initialization

```bash
kubebuilder init --domain standardchartered.com
kubebuilder create api --group gdp --version v1 --kind GTenant
make generate
make manifests
```

---

## Onboarding Workflow

```mermaid
flowchart TD
    subgraph Tenant Owner
        A1(submit platform.mf)
        A2(Ingest data via framework ingestion)
        A3(Download ETL framework Repo)
        A4(Update code and build artifact)
        A5(Upload artifact to S3)
        A6(Run Airflow RAG)
        A7(Query using Trino)
    end

    subgraph GDP Platform Team
        B1(read each tenant platform_mf)
        B2(Parse AppManifest in ado pipeline and trigger API)
        B3(Create GDPTenant CR and start onboard)
        C1(Create k8s namespace & quota)
        C2(Register minio bucket)
        C3(YuniKorn Spark job resource queue)
        C4(Trino computing resources)
        C5(Ranger permission policy)
        C6(Upload Pod template to s3)
    end

    A1 --> B1
    A1 --> B2
    A1 --> B3

    B3 --> C1
    B3 --> C2
    B3 --> C3
    B3 --> C4
    B3 --> C5
    B3 --> C6
    A2 --> A3
    A3 --> A4
    A4 --> A5
    A5 --> A6
    A6 --> A7
```

---

## Directory Structure

- `pkg/tenant/`
  Core logic for onboarding, resource creation, and integration with external systems.
- `pkg/model/`
  Data models and manifest definitions.
- `pkg/constants/`
  Project-wide constants.
- `charts/`
  Helm charts for deployment.
- `files/`
  Example manifests and configuration files.

---

## TODO

- Grafana dashboard integration
- ResourceQuota enhancements
- Pod limit enforcement
- Default configmap management
- Scheduled tenant reconciliation

---

## Contribution

Contributions are welcome! Please open issues or submit pull requests for improvements and bug fixes.

---

## License

This project is for internal use at Standard Chartered. For licensing or usage inquiries, please contact the GDP platform