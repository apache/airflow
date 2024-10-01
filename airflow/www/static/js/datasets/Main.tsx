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

import React, { useCallback, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Box,
  Breadcrumb,
  BreadcrumbLink,
  BreadcrumbItem,
  Heading,
  Tabs,
  Spinner,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Text,
  Flex,
  useDisclosure,
  IconButton,
} from "@chakra-ui/react";
import { HiDatabase } from "react-icons/hi";
import { MdEvent, MdAccountTree, MdDetails, MdPlayArrow } from "react-icons/md";

import Time from "src/components/Time";
import BreadcrumbText from "src/components/BreadcrumbText";
import { useOffsetTop } from "src/utils";
import { useAssetDependencies } from "src/api";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";
import Tooltip from "src/components/Tooltip";
import { useContainerRef } from "src/context/containerRef";

import DatasetEvents from "./AssetEvents";
import AssetsList from "./AssetsList";
import AssetDetails from "./AssetDetails";
import type { OnSelectProps } from "./types";
import Graph from "./Graph";
import SearchBar from "./SearchBar";
import CreateAssetEventModal from "./CreateAssetEvent";

const DATASET_URI_PARAM = "uri";
const DAG_ID_PARAM = "dag_id";
const TIMESTAMP_PARAM = "timestamp";
const TAB_PARAM = "tab";

const tabToIndex = (tab?: string) => {
  switch (tab) {
    case "graph":
      return 1;
    case "datasets":
      return 2;
    case "details":
    case "events":
    default:
      return 0;
  }
};

const indexToTab = (index: number, uri?: string) => {
  switch (index) {
    case 0:
      return uri ? "details" : "events";
    case 1:
      return "graph";
    case 2:
      if (!uri) return "datasets";
      return undefined;
    default:
      return undefined;
  }
};

const Datasets = () => {
  const contentRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(contentRef);
  const height = `calc(100vh - ${offsetTop + 100}px)`;

  const { data: datasetDependencies, isLoading } = useAssetDependencies();
  const [searchParams, setSearchParams] = useSearchParams();

  const { isOpen, onToggle, onClose } = useDisclosure();
  const containerRef = useContainerRef();

  const selectedUri = decodeURIComponent(
    searchParams.get(DATASET_URI_PARAM) || ""
  );

  const selectedTimestamp = decodeURIComponent(
    searchParams.get(TIMESTAMP_PARAM) || ""
  );
  const selectedDagId = searchParams.get(DAG_ID_PARAM) || undefined;

  const tab = searchParams.get(TAB_PARAM) || undefined;
  const tabIndex = tabToIndex(tab);

  const onChangeTab = useCallback(
    (index: number) => {
      const params = new URLSearchParamsWrapper(searchParams);
      const newTab = indexToTab(index, selectedUri);
      if (newTab) params.set(TAB_PARAM, newTab);
      else params.delete(TAB_PARAM);
      setSearchParams(params);
    },
    [setSearchParams, searchParams, selectedUri]
  );

  const onSelect = ({ uri, timestamp, dagId }: OnSelectProps = {}) => {
    if (dagId) {
      if (dagId === selectedDagId) searchParams.delete(DAG_ID_PARAM);
      searchParams.set(DAG_ID_PARAM, dagId);
      searchParams.delete(DATASET_URI_PARAM);
    } else if (uri) {
      searchParams.set(DATASET_URI_PARAM, uri);
      if (timestamp) searchParams.set(TIMESTAMP_PARAM, timestamp);
      else searchParams.delete(TIMESTAMP_PARAM);
      searchParams.delete(DAG_ID_PARAM);
      if (tab === "datasets") searchParams.delete(TAB_PARAM);
    } else {
      searchParams.delete(DATASET_URI_PARAM);
      searchParams.delete(TIMESTAMP_PARAM);
      searchParams.delete(DAG_ID_PARAM);
    }
    setSearchParams(searchParams);
  };

  return (
    <Box alignItems="flex-start" justifyContent="space-between">
      <Flex
        grow={1}
        justifyContent="space-between"
        alignItems="flex-end"
        p={3}
        pb={0}
      >
        <Breadcrumb
          mt={4}
          separator={
            <Heading as="h3" size="md" color="gray.300">
              /
            </Heading>
          }
        >
          <BreadcrumbItem>
            <BreadcrumbLink
              onClick={() => onSelect()}
              isCurrentPage={!selectedUri}
            >
              <Heading as="h3" size="md">
                Datasets
              </Heading>
            </BreadcrumbLink>
          </BreadcrumbItem>

          {selectedUri && (
            <BreadcrumbItem isCurrentPage={!!selectedUri && !selectedTimestamp}>
              <BreadcrumbLink onClick={() => onSelect({ uri: selectedUri })}>
                <BreadcrumbText label="URI" value={selectedUri} />
              </BreadcrumbLink>
            </BreadcrumbItem>
          )}

          {selectedTimestamp && (
            <BreadcrumbItem isCurrentPage={!!selectedTimestamp}>
              <BreadcrumbLink>
                <BreadcrumbText
                  label="Timestamp"
                  value={<Time dateTime={selectedTimestamp} />}
                />
              </BreadcrumbLink>
            </BreadcrumbItem>
          )}
        </Breadcrumb>
        {selectedUri && (
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
        )}
      </Flex>
      <Tabs ref={contentRef} isLazy index={tabIndex} onChange={onChangeTab}>
        <TabList>
          {!selectedUri && (
            <Tab>
              <MdEvent size={16} />
              <Text as="strong" ml={1}>
                Dataset Events
              </Text>
            </Tab>
          )}
          {!!selectedUri && (
            <Tab>
              <MdDetails size={16} />
              <Text as="strong" ml={1}>
                Details
              </Text>
            </Tab>
          )}
          <Tab>
            <MdAccountTree size={16} />
            <Text as="strong" ml={1}>
              Dependency Graph
            </Text>
          </Tab>
          {!selectedUri && (
            <Tab>
              <HiDatabase size={16} />
              <Text as="strong" ml={1}>
                Datasets
              </Text>
            </Tab>
          )}
        </TabList>
        <TabPanels>
          {!selectedUri && (
            <TabPanel>
              <DatasetEvents />
            </TabPanel>
          )}
          {!!selectedUri && (
            <TabPanel>
              <AssetDetails uri={selectedUri} />
            </TabPanel>
          )}
          <TabPanel>
            {isLoading && <Spinner position="absolute" top="50%" left="50%" />}
            {/* the graph needs a defined height to render properly */}
            <SearchBar
              datasetDependencies={datasetDependencies}
              selectedDagId={selectedDagId}
              selectedUri={selectedUri}
              onSelectNode={onSelect}
            />
            <Box
              flex={1}
              height={height}
              borderColor="gray.200"
              borderWidth={1}
              position="relative"
              mt={2}
            >
              {height && (
                <Graph
                  selectedNodeId={selectedUri || selectedDagId}
                  onSelect={onSelect}
                />
              )}
            </Box>
          </TabPanel>
          {!selectedUri && (
            <TabPanel>
              <AssetsList onSelect={onSelect} />
            </TabPanel>
          )}
        </TabPanels>
      </Tabs>
      {selectedUri && (
        <CreateAssetEventModal
          isOpen={isOpen}
          onClose={onClose}
          uri={selectedUri}
        />
      )}
    </Box>
  );
};

export default Datasets;
