# Apache Airflow provider for Hugging Face Datasets

![Hero Img](<docs/images/Apache Airflow ❤️ Hugging Face Datasets.png>)

Apache Airflow provider with operators to load, transform, stream, and publish Hugging Face Datasets.

## Compatibility

This release targets Airflow 3.x (`apache-airflow>=3.0,<4`) and Python 3.10–3.13. It uses Datasets 3.x/4.x and `huggingface-hub` 0.x/1.x public APIs. The provider is not published on PyPI. Install it from this repository into the same Python environment as Airflow:

```bash
pip install "apache-airflow-providers-huggingface @ git+https://github.com/ParagEkbote/hf-datasets-airflow-provider"
```

The root [`provider.yaml`](provider.yaml) records the provider metadata; runtime discovery uses Airflow's `apache_airflow_provider` entry point. See [connection setup](docs/connections.rst) and [operator guide](docs/operators.rst).

## End-to-end DAG

```python
from airflow.sdk import DAG, Asset
from my_project.transforms import normalize
from airflow.providers.huggingface.operators.datasets import (
    LoadDatasetOperator, MapDatasetOperator, PushToHubOperator,
)

published = Asset("hf://datasets/my-org/processed-demo")

with DAG("hf_dataset_pipeline") as dag:
    load = LoadDatasetOperator(
        task_id="load", path="stanfordnlp/imdb", split="train",
        revision="<commit-sha>", output_uri="file:///mnt/datasets/imdb.json",
    )
    transform = MapDatasetOperator(
        task_id="transform", input_uri=load.output,
        output_uri="file:///mnt/datasets/imdb_processed.json",
        transform=normalize, map_kwargs={"batched": True, "num_proc": 2},
    )
    publish = PushToHubOperator(
        task_id="publish", input_uri=transform.output,
        repo_id="my-org/processed-demo", private=True,
    )
    load >> transform >> publish

# In another DAG, schedule with `schedule=[published]`.
```

`my_project.transforms.normalize` must be importable wherever the DAG is parsed and on the task worker, and accept the Datasets map function signature. Large transformations execute in worker processes and need sufficient memory and disk. No dataset operation runs while parsing the DAG.

`PushToHubOperator` publishes the dataset through `Dataset.push_to_hub`, which updates the Hub repository README's YAML card metadata (including `dataset_info` and `configs`) in the push commit. The operator publishes data and lets Datasets update that metadata; it does not author dataset card content such as the README description or usage sections.

For a streaming pipeline, use `StreamDatasetOperator`. It applies transformations and filters lazily to an `IterableDataset` and progressively publishes the result to the Hub in one task, without building a Python list or an intermediate artifact. Use `LoadDatasetOperator` and `MapDatasetOperator` for durable local or object-storage artifacts.

## Examples

The [`examples/`](examples/) directory contains DAG examples for loading, mapping, and publishing datasets. See [operator examples](docs/operators.rst#example).
