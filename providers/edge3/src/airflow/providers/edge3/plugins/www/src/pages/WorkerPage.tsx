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
import {
  ActionBar,
  Box,
  Checkbox as ChakraCheckbox,
  CloseButton,
  Code,
  HStack,
  Link as ChakraLink,
  List,
  Portal,
  Table,
  Text,
  type SelectValueChangeDetails,
} from "@chakra-ui/react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useUiServiceWorker } from "openapi/queries";
import type { EdgeWorkerState, Worker } from "openapi/requests/types.gen";
import { Link } from "react-router-dom";
import { LuExternalLink } from "react-icons/lu";

import { BulkWorkerOperations } from "src/components/BulkWorkerOperations";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { WorkerOperations } from "src/components/WorkerOperations";
import { WorkerStateBadge } from "src/components/WorkerStateBadge";
import { ScrollToAnchor, Select } from "src/components/ui";
import { workerStateOptions } from "src/constants";
import { autoRefreshInterval } from "src/utils";
import { WorkerSysinfoBadge } from "src/components/WorkerSysinfoBadge";

export const WorkerPage = () => {
  const [workerNamePattern, setWorkerNamePattern] = useState("");
  const [queueNamePattern, setQueueNamePattern] = useState("");
  const [filteredState, setFilteredState] = useState<string[]>([]);
  const [selectedWorkerNames, setSelectedWorkerNames] = useState<Set<string>>(new Set());

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
  const workers = useMemo(() => data?.workers ?? [], [data?.workers]);

  useEffect(() => {
    setSelectedWorkerNames((previousSelectedWorkers) => {
      const availableWorkerNames = new Set(workers.map((worker) => worker.worker_name));
      const nextSelectedWorkers = new Set(
        [...previousSelectedWorkers].filter((workerName) => availableWorkerNames.has(workerName)),
      );

      if (nextSelectedWorkers.size === previousSelectedWorkers.size) {
        return previousSelectedWorkers;
      }

      return nextSelectedWorkers;
    });
  }, [workers]);

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
  const selectedWorkers = useMemo<Array<Worker>>(
    () => workers.filter((worker) => selectedWorkerNames.has(worker.worker_name)),
    [selectedWorkerNames, workers],
  );
  const selectedWorkersCount = selectedWorkers.length;
  const allWorkersSelected = workers.length > 0 && selectedWorkersCount === workers.length;
  const someWorkersSelected = selectedWorkersCount > 0 && !allWorkersSelected;

  const handleWorkerSelect = useCallback((workerName: string, selected: boolean) => {
    setSelectedWorkerNames((previousSelectedWorkers) => {
      const nextSelectedWorkers = new Set(previousSelectedWorkers);
      if (selected) {
        nextSelectedWorkers.add(workerName);
      } else {
        nextSelectedWorkers.delete(workerName);
      }

      return nextSelectedWorkers;
    });
  }, []);

  const handleSelectAllWorkers = useCallback(
    (selected: boolean) => {
      if (selected) {
        setSelectedWorkerNames(new Set(workers.map((worker) => worker.worker_name)));
      } else {
        setSelectedWorkerNames(new Set());
      }
    },
    [workers],
  );

  const clearSelections = useCallback(() => {
    setSelectedWorkerNames(new Set());
  }, []);

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
            isActive={hasFilteredState}
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
      ) : workers.length > 0 ? (
        <>
          <Table.Root size="sm" interactive stickyHeader striped>
            <Table.Header>
              <Table.Row>
                <Table.ColumnHeader width="44px">
                  <ChakraCheckbox.Root
                    checked={allWorkersSelected ? true : someWorkersSelected ? "indeterminate" : false}
                    colorPalette="brand"
                    onCheckedChange={(event) => handleSelectAllWorkers(event.checked === true)}
                  >
                    <ChakraCheckbox.HiddenInput />
                    <ChakraCheckbox.Control borderWidth={1} />
                  </ChakraCheckbox.Root>
                </Table.ColumnHeader>
                <Table.ColumnHeader>Worker Name</Table.ColumnHeader>
                <Table.ColumnHeader>State</Table.ColumnHeader>
                <Table.ColumnHeader>Queues</Table.ColumnHeader>
                <Table.ColumnHeader>Active Jobs</Table.ColumnHeader>
                <Table.ColumnHeader>System Status</Table.ColumnHeader>
                <Table.ColumnHeader>Operations</Table.ColumnHeader>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {workers.map((worker) => (
                <Table.Row key={worker.worker_name} id={worker.worker_name}>
                  <Table.Cell>
                    <ChakraCheckbox.Root
                      checked={selectedWorkerNames.has(worker.worker_name)}
                      colorPalette="brand"
                      onCheckedChange={(event) =>
                        handleWorkerSelect(worker.worker_name, event.checked === true)
                      }
                    >
                      <ChakraCheckbox.HiddenInput />
                      <ChakraCheckbox.Control borderWidth={1} />
                    </ChakraCheckbox.Root>
                  </Table.Cell>
                  <Table.Cell>{worker.worker_name}</Table.Cell>
                  <Table.Cell>
                    <WorkerStateBadge state={worker.state}>{worker.state}</WorkerStateBadge>
                  </Table.Cell>
                  <Table.Cell>
                    {worker.queues ? (
                      <List.Root>
                        {worker.queues.map((queue) => (
                          <List.Item key={queue}>
                            <Link relative="path" to={`../jobs?queue=${encodeURIComponent(queue)}`}>{queue}</Link>
                          </List.Item>
                        ))}
                      </List.Root>
                    ) : (
                      "(all queues)"
                    )}
                  </Table.Cell>
                  <Table.Cell>
                    {worker.jobs_active !== undefined && worker.jobs_active > 0 ? (
                      <Link relative="path" to={`../jobs?worker=${encodeURIComponent(worker.worker_name)}`}>
                        {worker.jobs_active}
                      </Link>
                    ) : (
                      worker.jobs_active ?? 0
                    )}
                  </Table.Cell>
                  <Table.Cell>
                    <WorkerSysinfoBadge sysinfo={worker.sysinfo} first_online={worker.first_online} last_heartbeat={worker.last_heartbeat}/>
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
          <ChakraLink
            target="_blank"
            variant="underline"
            color="fg.info"
            href="https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/deployment.html"
          >
            Edge Worker Deployment docs <LuExternalLink />
          </ChakraLink>{" "}
          how to deploy a new worker.
        </Text>
      )}
      <ActionBar.Root closeOnInteractOutside={false} open={Boolean(selectedWorkersCount)}>
        <Portal>
          <ActionBar.Positioner>
            <ActionBar.Content>
              <ActionBar.SelectionTrigger>
                {selectedWorkersCount} selected
              </ActionBar.SelectionTrigger>
              <ActionBar.Separator />
              <BulkWorkerOperations
                onClearSelection={clearSelections}
                onOperations={refetch}
                selectedWorkers={selectedWorkers}
              />
              <ActionBar.CloseTrigger asChild onClick={clearSelections}>
                <CloseButton size="sm" />
              </ActionBar.CloseTrigger>
            </ActionBar.Content>
          </ActionBar.Positioner>
        </Portal>
      </ActionBar.Root>
    </Box>
  );
};
