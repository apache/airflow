//# airflowBundle={"code":{"start":"00000000000003ec","end":"00000000000004fb","sha256":"fa1afadd7cb7147d54e4870b849dd2390e5ad75f6153d040e7381ae8a76a5a29"},"metadata":{"start":"00000000000001de","end":"00000000000002df","sha256":"0f522f6b96c0813da84ac9ef1e4c944b93da409b502c850386964d4117b8d4b0"},"sources":[{"path":"entry.ts","start":"00000000000002fb","end":"00000000000003e7","sha256":"3e7a601893ae2cc92dd72dcc596d3fbfa19cafbaa0d3f5768973c49c3daee1c9"}]}
//# airflowMetadata={"airflow_bundle_metadata_version":"1.0","sdk":{"language":"typescript","version":"0.1.0","supervisor_schema_version":"2026-06-16"},"entrypoint_path":"entry.ts","dag_source_paths":{"test_dag":"entry.ts"},"task_handlers":{"test_dag":{"tasks":["test_task"]}}}
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
