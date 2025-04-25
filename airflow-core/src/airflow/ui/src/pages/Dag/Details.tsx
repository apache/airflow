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
import { Box, Code, HStack, Table } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails } from "openapi/queries";
import { DagVersionDetails } from "src/components/DagVersionDetails";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";
import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";

export const Details = () => {
  const { dagId = "" } = useParams();

  const { data: dag } = useDagServiceGetDagDetails({
    dagId,
  });

  return (
    <Box p={2}>
      {dag === undefined ? (
        <div />
      ) : (
        <Table.Root striped>
          <Table.Body>
            <Table.Row>
              <Table.Cell>Dag ID</Table.Cell>
              <Table.Cell>
                <HStack>
                  {dag.dag_id}
                  <ClipboardRoot value={dag.dag_id}>
                    <ClipboardIconButton />
                  </ClipboardRoot>
                </HStack>
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Description</Table.Cell>
              <Table.Cell>{dag.description}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Timezone</Table.Cell>
              <Table.Cell>{dag.timezone}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>File Location</Table.Cell>
              <Table.Cell>
                <Code>{dag.fileloc}</Code>
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Last Parsed</Table.Cell>
              <Table.Cell>
                <Time datetime={dag.last_parsed} />
              </Table.Cell>
            </Table.Row>
            {dag.bundle_version !== null && (
              <Table.Row>
                <Table.Cell>Bundle Version</Table.Cell>
                <Table.Cell>{dag.bundle_version}</Table.Cell>
              </Table.Row>
            )}
            <Table.Row>
              <Table.Cell>Latest Dag Version</Table.Cell>
              <Table.Cell>
                <DagVersionDetails dagVersion={dag.latest_dag_version} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Start Date</Table.Cell>
              <Table.Cell>
                <Time datetime={dag.start_date} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>End Date</Table.Cell>
              <Table.Cell>
                <Time datetime={dag.end_date} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Last Expired</Table.Cell>
              <Table.Cell>
                <Time datetime={dag.last_expired} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Concurrency</Table.Cell>
              <Table.Cell>{dag.concurrency}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Has Task Concurrency Limits</Table.Cell>
              <Table.Cell>{dag.has_task_concurrency_limits.toString()}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Dag Run Timeout</Table.Cell>
              <Table.Cell>{dag.dag_run_timeout}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Max Active Runs</Table.Cell>
              <Table.Cell>{dag.max_active_runs}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Max Active Tasks</Table.Cell>
              <Table.Cell>{dag.max_active_tasks}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Max Consecutive Failed Dag Runs</Table.Cell>
              <Table.Cell>{dag.max_consecutive_failed_dag_runs}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>Catchup</Table.Cell>
              <Table.Cell>{dag.catchup.toString()}</Table.Cell>
            </Table.Row>
            {dag.params === null ? undefined : (
              <Table.Row>
                <Table.Cell>Params</Table.Cell>
                <Table.Cell>
                  <RenderedJsonField content={dag.params} />
                </Table.Cell>
              </Table.Row>
            )}
          </Table.Body>
        </Table.Root>
      )}
    </Box>
  );
};
