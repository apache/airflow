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
import { Box, Text, Table, Heading } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails } from "openapi/queries";
import Time from "src/components/Time";

export const Details = () => {
  const { dagId = "" } = useParams();

  const { data: dag } = useDagServiceGetDagDetails({
    dagId,
  });

  if (dag === undefined) {
    return <div />;
  }

  return (
    <Box p={2}>
      <Heading py={2} size="sm">
        DAG Details
      </Heading>
      <Table.Root striped>
        <Table.Body>
          <Table.Row>
            <Table.Cell>Dag display name</Table.Cell>
            <Table.Cell>{dag.dag_display_name}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Dag id</Table.Cell>
            <Table.Cell>{dag.dag_id}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Description</Table.Cell>
            <Table.Cell>{dag.description}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Fileloc</Table.Cell>
            <Table.Cell>{dag.fileloc}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Has import errors</Table.Cell>
            <Table.Cell>{dag.has_import_errors ? "True" : "False"}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Has task concurrency limits</Table.Cell>
            <Table.Cell>{dag.has_task_concurrency_limits ? "True" : "False"}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Is active</Table.Cell>
            <Table.Cell>{dag.is_active ? "True" : "False"}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Is paused</Table.Cell>
            <Table.Cell>{dag.is_paused ? "True" : "False"}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Last expired</Table.Cell>
            <Table.Cell>{dag.last_expired}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Last parsed time</Table.Cell>
            <Table.Cell>
              <Time datetime={dag.last_parsed_time} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Max active runs</Table.Cell>
            <Table.Cell>{dag.max_active_runs}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Max active tasks</Table.Cell>
            <Table.Cell>{dag.max_active_tasks}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Max consecutive failed dag runs</Table.Cell>
            <Table.Cell>{dag.max_consecutive_failed_dag_runs}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Next dagrun</Table.Cell>
            <Table.Cell>
              <Time datetime={dag.next_dagrun} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Next dagrun create after</Table.Cell>
            <Table.Cell>
              <Time datetime={dag.next_dagrun_create_after} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Next dagrun data interval end</Table.Cell>
            <Table.Cell>
              <Time datetime={dag.next_dagrun_data_interval_end} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Next dagrun data interval start</Table.Cell>
            <Table.Cell>
              <Time datetime={dag.next_dagrun_data_interval_start} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Timetable description</Table.Cell>
            <Table.Cell>{dag.timetable_description}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Asset expression</Table.Cell>
            <Table.Cell>{JSON.stringify(dag.asset_expression)}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Catchup</Table.Cell>
            <Table.Cell>{dag.catchup}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Doc md</Table.Cell>
            <Table.Cell>{dag.doc_md}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>End date</Table.Cell>
            <Table.Cell>
              <Time datetime={dag.end_date} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Is paused upon creation</Table.Cell>
            <Table.Cell>{dag.is_paused_upon_creation}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Last parsed</Table.Cell>
            <Table.Cell>
              <Time datetime={dag.last_parsed} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Render template as native obj</Table.Cell>
            <Table.Cell>{dag.render_template_as_native_obj}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Start date</Table.Cell>
            <Table.Cell>
              <Time datetime={dag.start_date} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Template search path</Table.Cell>
            <Table.Cell>{dag.template_search_path}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Timezone</Table.Cell>
            <Table.Cell>{dag.timezone}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Owners</Table.Cell>
            <Table.Cell>{dag.owners.join(", ")}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Tags</Table.Cell>
            <Table.Cell>
              {dag.tags.length ? <Text>{dag.tags.map((tag) => tag.name).join(", ")}</Text> : undefined}
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Timetable</Table.Cell>
            <Table.Cell>{dag.timetable_summary}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Dag run timeout</Table.Cell>
            <Table.Cell>{dag.dag_run_timeout}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Params</Table.Cell>
            <Table.Cell>{JSON.stringify(dag.params)}</Table.Cell>
          </Table.Row>
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
