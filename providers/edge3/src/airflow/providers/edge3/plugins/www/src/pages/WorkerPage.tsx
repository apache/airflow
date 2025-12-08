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
import { Box, Code, HStack, Link, List, Table, Text, type SelectValueChangeDetails } from "@chakra-ui/react";
import { useState, useCallback } from "react";
import { useUiServiceWorker } from "openapi/queries";
import { LuExternalLink } from "react-icons/lu";
import TimeAgo from "react-timeago";

import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { WorkerOperations } from "src/components/WorkerOperations";
import { WorkerStateBadge } from "src/components/WorkerStateBadge";
import { ScrollToAnchor, Select } from "src/components/ui";
import { workerStateOptions } from "src/constants";
import { autoRefreshInterval } from "src/utils";
import type { EdgeWorkerState } from "openapi/requests/types.gen";

export const WorkerPage = () => {
  const [workerNamePattern, setWorkerNamePattern] = useState("");
  const [queueNamePattern, setQueueNamePattern] = useState("");
  const [filteredState, setFilteredState] = useState<string[]>([]);

  const hasFilteredState = filteredState.length > 0;

  const { data, error, refetch } = useUiServiceWorker(
    {
      queueNamePattern: queueNamePattern || undefined,
      state: hasFilteredState ? (filteredState as EdgeWorkerState[]) : undefined,
      workerNamePattern: workerNamePattern || undefined,
    },
    undefined,
    {
      enabled: true,
      refetchInterval: autoRefreshInterval,
    },
  );

  const handleWorkerSearchChange = (value: string) => {
    setWorkerNamePattern(value);
  };

  const handleQueueSearchChange = (value: string) => {
    setQueueNamePattern(value);
  };

  const handleStateChange = useCallback(({ value }: SelectValueChangeDetails<string>) => {
    const [val, ...rest] = value;

    if ((val === undefined || val === "all") && rest.length === 0) {
      setFilteredState([]);
    } else {
      setFilteredState(value.filter((state) => state !== "all"));
    }
  }, []);

  // TODO to make it proper
  // Use DataTable as component from Airflow-Core UI
  // Add sorting
  // Add filtering
  // Add links with filter to see jobs on worker
  // Add time zone support for time display
  // Translation?

  return (
    <Box p={2}>
      <HStack gap={4} mb={4}>
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={workerNamePattern}
          hideAdvanced
          hotkeyDisabled
          onChange={handleWorkerSearchChange}
          placeHolder="Search workers"
        />
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={queueNamePattern}
          hideAdvanced
          hotkeyDisabled
          onChange={handleQueueSearchChange}
          placeHolder="Search queues"
        />
        <Select.Root
          collection={workerStateOptions}
          maxW="450px"
          multiple
          onValueChange={handleStateChange}
          value={hasFilteredState ? filteredState : ["all"]}
        >
          <Select.Trigger
            {...(hasFilteredState ? { clearable: true } : {})}
            colorPalette="brand"
            isActive={Boolean(filteredState)}
          >
            <Select.ValueText>
              {() =>
                hasFilteredState ? (
                  <HStack flexWrap="wrap" fontSize="sm" gap="4px" paddingY="8px">
                    {filteredState.map((state) => (
                      <WorkerStateBadge key={state} state={state as EdgeWorkerState}>
                        {state}
                      </WorkerStateBadge>
                    ))}
                  </HStack>
                ) : (
                  "All States"
                )
              }
            </Select.ValueText>
          </Select.Trigger>
          <Select.Content>
            {workerStateOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.value === "all" ? (
                  option.label
                ) : (
                  <WorkerStateBadge state={option.value as EdgeWorkerState}>{option.label}</WorkerStateBadge>
                )}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </HStack>
      {error ? (
        <ErrorAlert error={error} />
      ) : !data ? (
        <Text as="div" pl={2} pt={1}>
          Loading...
        </Text>
      ) : data.workers && data.workers.length > 0 ? (
        <>
          <Table.Root size="sm" interactive stickyHeader striped>
            <Table.Header>
              <Table.Row>
                <Table.ColumnHeader>Worker Name</Table.ColumnHeader>
                <Table.ColumnHeader>State</Table.ColumnHeader>
                <Table.ColumnHeader>Queues</Table.ColumnHeader>
                <Table.ColumnHeader>First Online</Table.ColumnHeader>
                <Table.ColumnHeader>Last Heartbeat</Table.ColumnHeader>
                <Table.ColumnHeader>Active Jobs</Table.ColumnHeader>
                <Table.ColumnHeader>System Information</Table.ColumnHeader>
                <Table.ColumnHeader>Operations</Table.ColumnHeader>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {data.workers.map((worker) => (
                <Table.Row key={worker.worker_name} id={worker.worker_name}>
                  <Table.Cell>{worker.worker_name}</Table.Cell>
                  <Table.Cell>
                    <WorkerStateBadge state={worker.state}>{worker.state}</WorkerStateBadge>
                  </Table.Cell>
                  <Table.Cell>
                    {worker.queues ? (
                      <List.Root>
                        {worker.queues.map((queue) => (
                          <List.Item key={queue}>{queue}</List.Item>
                        ))}
                      </List.Root>
                    ) : (
                      "(all queues)"
                    )}
                  </Table.Cell>
                  <Table.Cell>
                    {worker.first_online ? <TimeAgo date={worker.first_online} live={false} /> : undefined}
                  </Table.Cell>
                  <Table.Cell>
                    {worker.last_heartbeat ? <TimeAgo date={worker.last_heartbeat} live={false} /> : undefined}
                  </Table.Cell>
                  <Table.Cell>{worker.jobs_active}</Table.Cell>
                  <Table.Cell>
                    {worker.sysinfo ? (
                      <List.Root>
                        {Object.entries(worker.sysinfo).map(([key, value]) => (
                          <List.Item key={key}>
                            {key}: {value}
                          </List.Item>
                        ))}
                      </List.Root>
                    ) : (
                      "N/A"
                    )}
                  </Table.Cell>
                  <Table.Cell>
                    <WorkerOperations worker={worker} onOperations={refetch} />
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Root>
          <ScrollToAnchor />
        </>
      ) : (
        <Text as="div" pl={2} pt={1}>
          No known workers. Start one via <Code>airflow edge worker [...]</Code>. See{" "}
          <Link
            target="_blank"
            variant="underline"
            color="fg.info"
            href="https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/deployment.html"
          >
            Edge Worker Deployment docs <LuExternalLink />
          </Link>{" "}
          how to deploy a new worker.
        </Text>
      )}
    </Box>
  );
};
