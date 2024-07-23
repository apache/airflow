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

import React from "react";
import {
  Spinner,
  Flex,
  IconButton,
  useDisclosure,
  Grid,
  GridItem,
  Heading,
  Link,
  Box,
} from "@chakra-ui/react";
import { MdPlayArrow } from "react-icons/md";
import { isEmpty } from "lodash";

import { useDataset } from "src/api";
import { useContainerRef } from "src/context/containerRef";
import Tooltip from "src/components/Tooltip";
import { getMetaValue } from "src/utils";
import RenderedJsonField from "src/components/RenderedJsonField";

import CreateDatasetEventModal from "./CreateDatasetEvent";
import Events from "./DatasetEvents";

const gridUrl = getMetaValue("grid_url");

interface Props {
  uri: string;
}

const DatasetDetails = ({ uri }: Props) => {
  const { data: dataset, isLoading } = useDataset({ uri });
  const { isOpen, onToggle, onClose } = useDisclosure();
  const containerRef = useContainerRef();

  const hasProducingTasks = !!dataset?.producingTasks?.length;
  const hasConsumingDags = !!dataset?.consumingDags?.length;

  return (
    <Flex flexDirection="column">
      {isLoading && <Spinner display="block" />}
      <Grid templateColumns="repeat(5, 1fr)">
        {hasProducingTasks && (
          <GridItem colSpan={hasConsumingDags ? 2 : 4}>
            <Heading size="sm">Tasks that update this Dataset</Heading>
            {dataset?.producingTasks?.map((task) => {
              if (!task.taskId || !task.dagId) return null;
              const url = `${gridUrl?.replace(
                "__DAG_ID__",
                task.dagId
              )}?&task_id=${encodeURIComponent(task.taskId)}`;
              return (
                <Link
                  key={`${task.dagId}.${task.taskId}`}
                  color="blue.600"
                  href={url}
                  display="block"
                >
                  {task.dagId}.{task.taskId}
                </Link>
              );
            })}
          </GridItem>
        )}
        {hasConsumingDags && (
          <GridItem colSpan={hasProducingTasks ? 2 : 4}>
            <Heading size="sm">DAGs that consume this Dataset</Heading>
            {dataset?.consumingDags?.map((dag) => {
              if (!dag.dagId) return null;
              const url = gridUrl?.replace("__DAG_ID__", dag.dagId);
              return (
                <Link
                  display="block"
                  key={`${dag.dagId}`}
                  color="blue.600"
                  href={url}
                >
                  {dag.dagId}
                </Link>
              );
            })}
          </GridItem>
        )}
        <GridItem colSpan={1} display="flex" justifyContent="flex-end">
          <Tooltip
            label="Manually create dataset event"
            hasArrow
            portalProps={{ containerRef }}
          >
            <IconButton
              variant="outline"
              colorScheme="blue"
              aria-label="Manually create dataset event"
              onClick={onToggle}
            >
              <MdPlayArrow />
            </IconButton>
          </Tooltip>
        </GridItem>
      </Grid>
      {dataset?.extra && !isEmpty(dataset?.extra) && (
        <RenderedJsonField
          content={dataset.extra}
          bg="gray.100"
          maxH="300px"
          overflow="auto"
        />
      )}
      <Box mt={2}>
        {dataset && dataset.id && <Events datasetId={dataset.id} showLabel />}
      </Box>
      {dataset && (
        <CreateDatasetEventModal
          isOpen={isOpen}
          onClose={onClose}
          dataset={dataset}
        />
      )}
    </Flex>
  );
};

export default DatasetDetails;
