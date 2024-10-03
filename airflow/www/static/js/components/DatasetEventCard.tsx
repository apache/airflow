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
import { isEmpty } from "lodash";
import { TbApi } from "react-icons/tb";

import type { AssetEvent } from "src/types/api-generated";
import {
  Box,
  Flex,
  Tooltip,
  Text,
  Grid,
  GridItem,
  Link,
} from "@chakra-ui/react";
import { HiDatabase } from "react-icons/hi";
import { useSearchParams } from "react-router-dom";

import { getMetaValue } from "src/utils";
import Time from "src/components/Time";
import { useContainerRef } from "src/context/containerRef";
import RenderedJsonField from "src/components/RenderedJsonField";

import SourceTaskInstance from "./SourceTaskInstance";
import TriggeredDagRuns from "./TriggeredDagRuns";

type CardProps = {
  assetEvent: AssetEvent;
  showSource?: boolean;
  showTriggeredDagRuns?: boolean;
};

const datasetsUrl = getMetaValue("datasets_url");

const DatasetEventCard = ({
  assetEvent,
  showSource = true,
  showTriggeredDagRuns = true,
}: CardProps) => {
  const [searchParams] = useSearchParams();

  const selectedUri = decodeURIComponent(searchParams.get("uri") || "");
  const containerRef = useContainerRef();

  const { from_rest_api: fromRestApi, ...extra } = assetEvent?.extra as Record<
    string,
    string
  >;

  return (
    <Box>
      <Grid
        templateColumns="repeat(4, 1fr)"
        key={`${assetEvent.datasetId}-${assetEvent.timestamp}`}
        _hover={{ bg: "gray.50" }}
        transition="background-color 0.2s"
        p={2}
        borderTopWidth={1}
        borderColor="gray.300"
        borderStyle="solid"
      >
        <GridItem colSpan={2}>
          <Time dateTime={assetEvent.timestamp} />
          <Flex alignItems="center">
            <HiDatabase size="16px" />
            {assetEvent.datasetUri && assetEvent.datasetUri !== selectedUri ? (
              <Link
                color="blue.600"
                ml={2}
                href={`${datasetsUrl}?uri=${encodeURIComponent(
                  assetEvent.datasetUri
                )}`}
              >
                {assetEvent.datasetUri}
              </Link>
            ) : (
              <Text ml={2}>{assetEvent.datasetUri}</Text>
            )}
          </Flex>
        </GridItem>
        <GridItem>
          {showSource && (
            <>
              Source:
              {fromRestApi && (
                <Tooltip
                  portalProps={{ containerRef }}
                  hasArrow
                  placement="top"
                  label="Manually created from REST API"
                >
                  <Box width="20px">
                    <TbApi size="20px" />
                  </Box>
                </Tooltip>
              )}
              {!!assetEvent.sourceTaskId && (
                <SourceTaskInstance assetEvent={assetEvent} />
              )}
            </>
          )}
        </GridItem>
        <GridItem>
          {showTriggeredDagRuns && !!assetEvent?.createdDagruns?.length && (
            <>
              Triggered Dag Runs:
              <TriggeredDagRuns createdDagRuns={assetEvent?.createdDagruns} />
            </>
          )}
        </GridItem>
      </Grid>
      {!isEmpty(extra) && (
        <RenderedJsonField
          content={extra}
          bg="gray.100"
          maxH="300px"
          overflow="auto"
          jsonProps={{
            collapsed: true,
          }}
        />
      )}
    </Box>
  );
};

export default DatasetEventCard;
