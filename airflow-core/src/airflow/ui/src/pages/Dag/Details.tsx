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
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails } from "openapi/queries";
import { DagVersionDetails } from "src/components/DagVersionDetails";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";
import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";

export const Details = () => {
  const { t: translate } = useTranslation("dag");
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
              <Table.Cell>{translate("details.fields.dagId")}</Table.Cell>
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
              <Table.Cell>{translate("details.fields.description")}</Table.Cell>
              <Table.Cell>{dag.description}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.timezone")}</Table.Cell>
              <Table.Cell>{dag.timezone}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.fileLocation")}</Table.Cell>
              <Table.Cell>
                <Code>{dag.fileloc}</Code>
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.lastParsed")}</Table.Cell>
              <Table.Cell>
                <Time datetime={dag.last_parsed} />
              </Table.Cell>
            </Table.Row>
            {dag.bundle_version !== null && (
              <Table.Row>
                <Table.Cell>{translate("details.fields.bundleVersion")}</Table.Cell>
                <Table.Cell>{dag.bundle_version}</Table.Cell>
              </Table.Row>
            )}
            <Table.Row>
              <Table.Cell>{translate("details.fields.latestDagVersion")}</Table.Cell>
              <Table.Cell>
                <DagVersionDetails dagVersion={dag.latest_dag_version} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.startDate")}</Table.Cell>
              <Table.Cell>
                <Time datetime={dag.start_date} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.endDate")}</Table.Cell>
              <Table.Cell>
                <Time datetime={dag.end_date} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.lastExpired")}</Table.Cell>
              <Table.Cell>
                <Time datetime={dag.last_expired} />
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.concurrency")}</Table.Cell>
              <Table.Cell>{dag.concurrency}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.hasTaskConcurrencyLimits")}</Table.Cell>
              <Table.Cell>{dag.has_task_concurrency_limits.toString()}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.dagRunTimeout")}</Table.Cell>
              <Table.Cell>{dag.dag_run_timeout}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.maxActiveRuns")}</Table.Cell>
              <Table.Cell>{dag.max_active_runs}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.maxActiveTasks")}</Table.Cell>
              <Table.Cell>{dag.max_active_tasks}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.maxConsecutiveFailedDagRuns")}</Table.Cell>
              <Table.Cell>{dag.max_consecutive_failed_dag_runs}</Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell>{translate("details.fields.catchup")}</Table.Cell>
              <Table.Cell>{dag.catchup.toString()}</Table.Cell>
            </Table.Row>
            {dag.default_args === null ? undefined : (
              <Table.Row>
                <Table.Cell>{translate("details.fields.defaultArgs")}</Table.Cell>
                <Table.Cell>
                  <RenderedJsonField content={dag.default_args} />
                </Table.Cell>
              </Table.Row>
            )}
            {dag.params === null ? undefined : (
              <Table.Row>
                <Table.Cell>{translate("details.fields.params")}</Table.Cell>
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
