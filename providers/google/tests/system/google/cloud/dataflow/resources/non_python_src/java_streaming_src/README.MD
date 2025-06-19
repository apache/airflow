# Dataflow Java Streaming

This project is a streaming example running with latest Apache Beam Java SDK (v.2.63),
as there is no "official" streaming example yet https://beam.apache.org/get-started/wordcount-example/.

Sample logic to direct streaming data from input Pub/Sub source to the output PubSub topic to work
with unbounded data source/destination.

That used for Java dataflow streaming system tests.


## Requirements

- **Java 17**: Ensure you have Java 17 installed and configured on your system.
- **Maven**: Make sure Maven is installed and configured on your system.
- Maven is used for dependency management and building the project.

## Project Structure

The project's structure is as follows:

```plaintext
├── src
│   ├── main
│   │   ├── java
│   │   │   └── org
│   │   │       └── example
│   │   │           └── pubsub
│   │   │               └── StreamingExample.java
├── pom.xml
└── README.md
```

## Build
It was checked to build inside Breeze container with dependencies installed from the [requirements](#requirements).

The output artifact is `target/stream-pubsub-example-bundled-v-0.1.jar` executable.


## Run
To run use the
```bash
java -jar target/stream_pubsub-bundled-sample-0.1.jar \
--runner=DataflowRunner \
--project=<project_id> \
--region=<location_id> \
--jobName=<df_job_name> \
--input_toopic=<input_PubSub_topic> \
--output_topic=<input_PubSub_topic>
```

optionally you might add the
`'--labels=<custom_labels_dict>'` or  `--tempLocation=<gcs_path>` and  `--stagingLocation=<gcs_path>`
or other dataflow pipeline options, if needed.


## Runners
The `DataflowRunner` and `DirectRunner` are supported.
