
graph TB
    subgraph "Data Source"
        A[Chicago Crime API<br/>Socrata Open Data]
    end
    
    subgraph "AWS - Ingestion Layer"
        B[EventBridge<br/>Daily Schedule]
        C[Lambda Function<br/>crime_poller.py]
        D[Kinesis Firehose<br/>Buffering & Compression]
    end
    
    subgraph "AWS - Storage Layer"
        E[S3 Raw Bucket<br/>GZIP NDJSON<br/>dt=YYYY-MM-DD partitions]
        F[S3 Curated Bucket<br/>Apache Iceberg Tables<br/>Parquet + Metadata]
    end
    
    subgraph "AWS - Transform Layer"
        G[Glue PySpark Job<br/>Validation & Enrichment]
        H[Glue Data Catalog<br/>chicrime.crimes_iceberg]
    end
    
    subgraph "AWS - Governance Layer"
        I[Lake Formation<br/>Column/Row Security]
        J[CloudTrail<br/>Audit Logging]
    end
    
    subgraph "AWS - Analytics Layer"
        K[Athena<br/>Serverless SQL]
        L[QuickSight<br/>Optional Dashboards]
    end
    
    subgraph "AWS - Operations"
        M[CloudWatch<br/>Logs & Metrics]
        N[Step Functions<br/>Nightly Compaction]
    end
    
    A -->|HTTPS GET| C
    B -->|Triggers Daily| C
    C -->|PutRecord Batches| D
    D -->|Writes GZIP| E
    E -->|Reads Raw JSON| G
    G -->|Writes Iceberg| F
    G -->|Registers Schema| H
    F --> I
    H --> I
    I -->|Enforces Policies| K
    K -->|Queries Iceberg| F
    K -->|Optional| L
    C -->|Logs| M
    G -->|Logs| M
    K -->|Metrics| M
    N -->|Maintains| F
    I -->|Logs Access| J
    
    style A fill:#e1f5ff
    style E fill:#fff4e6
    style F fill:#e8f5e9
    style K fill:#f3e5f5
    style I fill:#ffebee