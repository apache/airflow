/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export type DagParams = {
  configJson: string;
  dagId: string;
  dataIntervalEnd: string;
  dataIntervalStart: string;
  runId: string;
};

export type TriggerDAGButtonProps = {
  dagDisplayName: string;
  dagId: string;
};

export const TriggerDag = (dagParams: DagParams) => {
  // eslint-disable-next-line no-alert
  alert(`
    Triggering DAG with the following parameters:

    Config JSON: ${JSON.stringify(dagParams.configJson)}
    Data Interval Start Date: ${dagParams.dataIntervalStart}
    Data Interval End Date: ${dagParams.dataIntervalEnd}
    Run ID: ${dagParams.runId}

    TODO: This trigger button is under progress.
    The values you have entered are shown above.
  `);

  // TODO triggering logic (would be placed here once the FAST API is available)
  console.log("Triggering DAG with parameters:", dagParams);
};
