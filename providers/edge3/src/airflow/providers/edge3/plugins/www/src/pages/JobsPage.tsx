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
import { Box, Link, Table, Text } from "@chakra-ui/react";
import { useUiServiceJobs } from "openapi/queries";
import { Link as RouterLink } from "react-router-dom";
import TimeAgo from "react-timeago";

import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import { autoRefreshInterval } from "src/utils";

export const JobsPage = () => {
  const { data, error } = useUiServiceJobs(undefined, {
    enabled: true,
    refetchInterval: autoRefreshInterval,
  });

  // TODO to make it proper
  // Use DataTable as component from Airflow-Core UI
  // Add sorting
  // Add filtering
  // Translation?
  if (data?.jobs && data.jobs.length > 0)
    return (
      <Box p={2}>
        <Table.Root size="sm" interactive stickyHeader striped>
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeader>Dag ID</Table.ColumnHeader>
              <Table.ColumnHeader>Run ID</Table.ColumnHeader>
              <Table.ColumnHeader>Task ID</Table.ColumnHeader>
              <Table.ColumnHeader>Map Index</Table.ColumnHeader>
              <Table.ColumnHeader>Try Number</Table.ColumnHeader>
              <Table.ColumnHeader>State</Table.ColumnHeader>
              <Table.ColumnHeader>Queue</Table.ColumnHeader>
              <Table.ColumnHeader>Queued DTTM</Table.ColumnHeader>
              <Table.ColumnHeader>Edge Worker</Table.ColumnHeader>
              <Table.ColumnHeader>Last Update</Table.ColumnHeader>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {data.jobs.map((job) => (
              <Table.Row
                key={`${job.dag_id}.${job.run_id}.${job.task_id}.${job.map_index}.${job.try_number}`}
              >
                <Table.Cell>
                  {/* TODO Check why <Link to={`/dags/${job.dag_id}`}> is not working via react-router-dom! */}
                  <Link href={`../dags/${job.dag_id}`}>{job.dag_id}</Link>
                </Table.Cell>
                <Table.Cell>
                  <Link href={`../dags/${job.dag_id}/runs/${job.run_id}`}>{job.run_id}</Link>
                </Table.Cell>
                <Table.Cell>
                  {job.map_index >= 0 ? (
                    <Link
                      href={`../dags/${job.dag_id}/runs/${job.run_id}/tasks/${job.task_id}/mapped/${job.map_index}?try_number=${job.try_number}`}
                    >
                      {job.task_id}
                    </Link>
                  ) : (
                    <Link
                      href={`../dags/${job.dag_id}/runs/${job.run_id}/tasks/${job.task_id}?try_number=${job.try_number}`}
                    >
                      {job.task_id}
                    </Link>
                  )}
                </Table.Cell>
                <Table.Cell>{job.map_index >= 0 ? job.map_index : "-"}</Table.Cell>
                <Table.Cell>{job.try_number}</Table.Cell>
                <Table.Cell>
                  <StateBadge state={job.state}>{job.state}</StateBadge>
                </Table.Cell>
                <Table.Cell>{job.queue}</Table.Cell>
                <Table.Cell>
                  {job.queued_dttm ? <TimeAgo date={job.queued_dttm} live={false} /> : undefined}
                </Table.Cell>
                <Table.Cell>
                  <RouterLink to={`/plugin/edge_worker#${job.edge_worker}`}>{job.edge_worker}</RouterLink>
                </Table.Cell>
                <Table.Cell>
                  {job.last_update ? <TimeAgo date={job.last_update} live={false} /> : undefined}
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      </Box>
    );
  if (data) {
    return (
      <Text as="div" pl={4} pt={1}>
        Currently no jobs running. Start a Dag and then all active jobs should show up here. Note that after
        some (configurable) time, jobs are purged from the list.
      </Text>
    );
  }
  if (error) {
    return (
      <Text as="div" pl={4} pt={1}>
        <ErrorAlert error={error} />
      </Text>
    );
  }
  return (
    <Text as="div" pl={4} pt={1}>
      Loading...
    </Text>
  );
};
