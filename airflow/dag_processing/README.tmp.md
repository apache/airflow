# DagBag refactoring

DAG metadata -> load from DB             -> DAG store
DAG discover & load -> parse from files  -> DAG importer
DAG sync to DB -> save to DB             -> DAG store
DAG apply policies                       -> DAG importer


Airflow 2-sh: FS -> DAG importer (how to generate DAG from source) -> DAG ingester (how/when store the DAG) -> DAG parser - top-level control
Airflow 3-sh: DAG bundle -> DAG importer (how to generate DAG from source) -> DAG ingester (how/when store the DAG) -> DAG parser - top-level control


Sample manifest.yaml:


```yaml
config:
   ingester: CompositeIngester
    composites:
      - ingester: OnceIngester
        paths:
          - path: /files/dags/static_1
          - path: /files/dags/static_2
            bundle:
              type: GitDagBundle
              repo_url: git://...
              tracking_ref: live
              refresh_interval: 10m
      - ingestion_options:
          ingestion_interval: 5m
        paths:
          - path: /files/dags/dynamic_1
      - ingestion_options:
          on_change_only: true
        paths:
          - path: /files/dags/dynamic_2
      - paths:
          - path: /files/dags/notebooks
            importer: NotebooksImporter
```

Which will expand to:

```yaml
config:
    ingester: CompositeIngester
    composites:
        - ingester: OnceIngester
        ingestion_options: {}
        paths:
          - path: /files/dags/static_1
            importer: FSDagImporter
            bundle:
                type: LocalDagBundle
          - path: /files/dags/static_2
            importer: FSDagImporter
            bundle:
                type: GitDagBundle
                repo_url: git://...
                tracking_ref: live
                refresh_interval: 10m
      - ingester: ContinousIngester
        ingestion_options:
            ingestion_interval: 5m
        paths:
          - path: /files/dags/dynamic_1
            importer: FSDagImporter
            bundle:
                type: LocalDagBundle
      - ingester: ContinousIngester
        ingestion_options:
            on_change_only: true
        paths:
          - path: /files/dags/dynamic_2
            importer: FSDagImporter
            bundle:
                type: LocalDagBundle
      - ingester: ContinousIngester
        ingestion_options: {}
        paths:
          - path: /files/dags/notebooks
            importer: NotebooksImporter
            bundle:
                type: LocalDagBundle
```

Default manifest is effectively:

```yaml
config:
    ingester: ContinousIngester
    ingestion_options: {}
    paths:
      - path: <DAG Folder>
        importer: FSDagImporter
        bundle:
            type: LocalDagBundle
```

Manifest spec (pseudocode):
```yaml
schema:
    config:
        type: object[ParsingSpec]
        properties:
            ingester:
                type: string # Ingester class name
              ingestion_options:
                type: object[IngesterParameters]
                properties:
                  ... # Ingester-dependent properties
            composites:
                type: array[ParsingSpec] # Nested parsing specs
            paths:
                type: array[PathSpec]
                properties:
                    path: # Path to import DAGs from, can be path in FS, can be virtual, if importer understands it
                      type: string
                    importer: # Importer class to use
                      type: string
                    bundle: # Bundle config, responsible for supplying requested data to importer
                      type: object[BundleSpec]
                      properties:
                        type: # Bundle class
                          type: string
                        ... # Type-dependent properties   
```
