//# airflowBundle={"code":{"start":"00000000000003b2","end":"000000000000051f","sha256":"8af7d9076186189a147afc538673260fc264f8e91d301b80a25a4f911ff3a00c"},"metadata":{"start":"00000000000001c9","end":"0000000000000296","sha256":"93ef7891d3e562383d4fd95332d9cf3b0662e581623dff811443350d16a84479"},"source":{"start":"00000000000002a9","end":"00000000000003ad","sha256":"4b88a98708f59dab99292eea330cc38971c1c496279d1b5aa6de22a708dbd8d9"}}
//# airflowMetadata={"airflow_bundle_metadata_version":"1.0","sdk":{"language":"typescript","version":"0.1.0","supervisor_schema_version":"2026-06-16"},"source":"entry.ts","task_handlers":{"test_dag":{"tasks":["test_task"]}}}
/*# airflowSource
/** Handlers for the test Dag. *\/
import { Dag, DagRegistry, serveDags } from "apache-airflow-ts-sdk";

const TERMINATOR = /\*\\//;
const dag = new Dag("test_dag");
dag.task("test_task", async () => TERMINATOR.source);

await serveDags(new DagRegistry(dag));

#*/
var e=Object.defineProperty;var o=(r,t)=>e(r,"name",{value:t,configurable:!0});var a=o(async function(){return"extracted"},"test_task");await a();
/*! Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
