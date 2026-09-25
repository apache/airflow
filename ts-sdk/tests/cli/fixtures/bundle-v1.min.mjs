//# airflowBundle={"code":{"start":"00000000000003e7","end":"00000000000004f6","sha256":"fa1afadd7cb7147d54e4870b849dd2390e5ad75f6153d040e7381ae8a76a5a29"},"metadata":{"start":"00000000000001de","end":"00000000000002da","sha256":"42659edbd65f526732fa90c9fca26246c190451ea8f38610346c7747d62c9b79"},"sources":[{"path":"entry.ts","start":"00000000000002f6","end":"00000000000003e2","sha256":"3e7a601893ae2cc92dd72dcc596d3fbfa19cafbaa0d3f5768973c49c3daee1c9"}]}
//# airflowMetadata={"airflow_bundle_metadata_version":"1.0","sdk":{"language":"typescript","version":"0.1.0","supervisor_schema_version":"2026-06-16"},"entrypoint":"entry.ts","dag_source_paths":{"test_dag":"entry.ts"},"task_handlers":{"test_dag":{"tasks":["test_task"]}}}
/*# airflowSource:entry.ts
/** Handlers for the test Dag. *\/
import { Bundle, Dag } from "apache-airflow-ts-sdk";

const TERMINATOR = /\*\\//;
const dag = new Dag("test_dag");
dag.task("test_task", async () => TERMINATOR.source);

await new Bundle(dag).serve();

#*/
var e=async function(){return"extracted"};await e();
/*! Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
