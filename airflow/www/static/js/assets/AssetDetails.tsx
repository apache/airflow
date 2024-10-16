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
  Grid,
  GridItem,
  Heading,
  Link,
  Box,
} from "@chakra-ui/react";
import { isEmpty } from "lodash";

import { useAsset } from "src/api";
import { getMetaValue } from "src/utils";
import RenderedJsonField from "src/components/RenderedJsonField";

import Events from "./AssetEvents";

const gridUrl = getMetaValue("grid_url");

interface Props {
  uri: string;
}

const AssetDetails = ({ uri }: Props) => {
  const { data: asset, isLoading } = useAsset({ uri });

  const hasProducingTasks = !!asset?.producingTasks?.length;
  const hasConsumingDags = !!asset?.consumingDags?.length;

  return (
    <Flex flexDirection="column">
      {isLoading && <Spinner display="block" />}
      <Grid templateColumns="repeat(5, 1fr)">
        {hasProducingTasks && (
          <GridItem colSpan={hasConsumingDags ? 2 : 4}>
            <Heading size="sm">Tasks that update this Asset</Heading>
            {asset?.producingTasks?.map((task) => {
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
            <Heading size="sm">DAGs that consume this Asset</Heading>
            {asset?.consumingDags?.map((dag) => {
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
      </Grid>
      {asset?.extra && !isEmpty(asset?.extra) && (
        <RenderedJsonField
          content={asset.extra}
          bg="gray.100"
          maxH="300px"
          overflow="auto"
        />
      )}
      <Box mt={2}>
        {asset && asset.id && <Events assetId={asset.id} showLabel />}
      </Box>
    </Flex>
  );
};

export default AssetDetails;
