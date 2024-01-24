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

import React, { useMemo } from "react";
import {
  Flex,
  Code,
  Link,
  Box,
  Text,
  useDisclosure,
  ModalCloseButton,
  Modal,
  ModalContent,
  ModalOverlay,
  ModalBody,
  ModalHeader,
  IconButton,
  useToast,
  ModalFooter,
  Button,
} from "@chakra-ui/react";

import { Table } from "src/components/Table";
import Time from "src/components/Time";
import { getMetaValue } from "src/utils";
import { useContainerRef } from "src/context/containerRef";
import { SimpleStatus } from "src/dag/StatusBox";
import { FiTrash2 } from "react-icons/fi";
import { useDeleteDataset } from "src/api";

interface CellProps {
  cell: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    value: any;
    row: {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      original: Record<string, any>;
    };
  };
}

export const TimeCell = ({ cell: { value } }: CellProps) => (
  <Time dateTime={value} />
);

export const DatasetLink = ({ cell: { value } }: CellProps) => {
  const datasetsUrl = getMetaValue("datasets_url");
  return (
    <Link
      color="blue.600"
      href={`${datasetsUrl}?uri=${encodeURIComponent(value)}`}
    >
      {value}
    </Link>
  );
};

export const DagRunLink = ({ cell: { value, row } }: CellProps) => {
  const dagId = getMetaValue("dag_id");
  const gridUrl = getMetaValue("grid_url");
  const stringToReplace = dagId || "__DAG_ID__";
  const url = `${gridUrl?.replace(
    stringToReplace,
    value
  )}?dag_run_id=${encodeURIComponent(row.original.dagRunId)}`;
  return (
    <Flex alignItems="center">
      <SimpleStatus state={row.original.state} mr={2} />
      <Link color="blue.600" href={url}>
        {value}
      </Link>
    </Flex>
  );
};

export const TriggeredRuns = ({ cell: { value, row } }: CellProps) => {
  const { isOpen, onToggle, onClose } = useDisclosure();
  const containerRef = useContainerRef();

  const columns = useMemo(
    () => [
      {
        Header: "DAG Id",
        accessor: "dagId",
        Cell: DagRunLink,
      },
      {
        Header: "Logical Date",
        accessor: "logicalDate",
        Cell: TimeCell,
      },
    ],
    []
  );

  const data = useMemo(() => value, [value]);

  if (!value || !value.length) return null;

  return (
    <Box>
      <Text color="blue.600" cursor="pointer" onClick={onToggle}>
        {value.length}
      </Text>
      <Modal
        size="3xl"
        isOpen={isOpen}
        onClose={onClose}
        scrollBehavior="inside"
        blockScrollOnMount={false}
        portalProps={{ containerRef }}
      >
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>
            <Text as="span" color="gray.400">
              Dag Runs triggered by
            </Text>
            <br />
            {row.original.datasetUri}
            <br />
            <Text as="span" color="gray.400">
              at
            </Text>
            <br />
            <Time dateTime={row.original.timestamp} />
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Table data={data} columns={columns} pageSize={data.length} />
          </ModalBody>
        </ModalContent>
      </Modal>
    </Box>
  );
};

export const TaskInstanceLink = ({ cell: { value, row } }: CellProps) => {
  const { sourceRunId, sourceDagId, sourceMapIndex } = row.original;
  const gridUrl = getMetaValue("grid_url");
  const dagId = getMetaValue("dag_id");
  const stringToReplace = dagId || "__DAG_ID__";
  const url = `${gridUrl?.replace(
    stringToReplace,
    sourceDagId
  )}?dag_run_id=${encodeURIComponent(sourceRunId)}&task_id=${encodeURIComponent(
    value
  )}`;
  const mapIndex = sourceMapIndex > -1 ? `[${sourceMapIndex}]` : "";
  return (
    <Box>
      <Link
        color="blue.600"
        href={url}
      >{`${sourceDagId}.${value}${mapIndex}`}</Link>
      <Text>{sourceRunId}</Text>
    </Box>
  );
};

export const CodeCell = ({ cell: { value } }: CellProps) =>
  value ? <Code>{JSON.stringify(value)}</Code> : null;

export const DatasetActionCell = ({ cell: { row } }: CellProps) => {
  const {
    mutateAsync: apiCallToDeleteDataset,
    isLoading: deleteDatasetIsLoading,
  } = useDeleteDataset(row.original.uri);

  const toast = useToast();
  const { isOpen, onOpen, onClose } = useDisclosure();
  const containerRef = useContainerRef();

  return (
    <Flex alignItems="center" justifyContent="center">
      <IconButton
        icon={<FiTrash2 />}
        aria-label="Delete"
        variant="outline"
        title="Delete"
        colorScheme="red"
        isLoading={deleteDatasetIsLoading}
        onClick={(e) => {
          onOpen();
          e.stopPropagation();
        }}
      />
      <Modal
        size="3xl"
        isOpen={isOpen}
        onClose={onClose}
        scrollBehavior="inside"
        blockScrollOnMount={false}
        portalProps={{ containerRef }}
      >
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Delete Dataset</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            Are you sure you want to delete dataset <b>{row.original.uri}?</b>
          </ModalBody>

          <ModalFooter justifyContent="space-between">
            <Button colorScheme="gray" mr={3} onClick={onClose}>
              Cancel
            </Button>
            <Button
              aria-label="Confirm Delete"
              title="Confirm Delete"
              colorScheme="red"
              onClick={() => {
                toast.promise(apiCallToDeleteDataset(), {
                  success: { title: "Dataset deleted" },
                  error: {
                    duration: 0,
                    render: () => null, // will use errorToast util in mutation
                  },
                  loading: { title: "Deleting..." },
                });
                onClose();
              }}
            >
              Delete
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </Flex>
  );
};
