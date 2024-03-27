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

import React, { useCallback, useEffect, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import { Flex, Box, Spinner } from "@chakra-ui/react";

import { useOffsetTop } from "src/utils";
import { useDatasetDependencies } from "src/api";

import DatasetsList from "./List";
import DatasetDetails from "./Details";
import Graph from "./Graph";

const DATASET_URI_PARAM = "uri";
const DAG_ID_PARAM = "dag_id";
const minPanelWidth = 300;

const Datasets = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const contentRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(contentRef);
  const listRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<HTMLDivElement>(null);
  // 60px for footer height
  const height = `calc(100vh - ${offsetTop + 60}px)`;

  const resizeRef = useRef<HTMLDivElement>(null);

  const selectedUri = decodeURIComponent(
    searchParams.get(DATASET_URI_PARAM) || ""
  );
  const selectedDagId = searchParams.get(DAG_ID_PARAM);

  // We need to load in the raw dependencies in order to generate the list of dagIds
  const { data: datasetDependencies, isLoading } = useDatasetDependencies();

  const resize = useCallback(
    (e: MouseEvent) => {
      const listEl = listRef.current;
      if (
        listEl &&
        e.x > minPanelWidth &&
        e.x < window.innerWidth - minPanelWidth
      ) {
        const width = `${e.x}px`;
        listEl.style.width = width;
      }
    },
    [listRef]
  );

  useEffect(() => {
    const resizeEl = resizeRef.current;
    if (resizeEl) {
      resizeEl.addEventListener("mousedown", (e) => {
        e.preventDefault();
        document.addEventListener("mousemove", resize);
      });

      document.addEventListener("mouseup", () => {
        document.removeEventListener("mousemove", resize);
      });

      return () => {
        resizeEl?.removeEventListener("mousedown", resize);
        document.removeEventListener("mouseup", resize);
      };
    }
    return () => {};
  }, [resize]);

  const selectedNodeId = selectedUri || selectedDagId;

  const onSelectNode = (id: string, type: string) => {
    if (type === "dag") {
      if (id === selectedDagId) searchParams.delete(DAG_ID_PARAM);
      else searchParams.set(DAG_ID_PARAM, id);
      searchParams.delete(DATASET_URI_PARAM);
    }
    if (type === "dataset") {
      if (id === selectedUri) searchParams.delete(DATASET_URI_PARAM);
      else searchParams.set(DATASET_URI_PARAM, id);
      searchParams.delete(DAG_ID_PARAM);
    }
    setSearchParams(searchParams);
  };

  return (
    <Flex
      alignItems="flex-start"
      justifyContent="space-between"
      ref={contentRef}
    >
      <Box
        minWidth={minPanelWidth}
        width={500}
        height={height}
        overflowY="auto"
        ref={listRef}
        mr={3}
      >
        {selectedUri ? (
          <DatasetDetails
            uri={selectedUri}
            onBack={() => onSelectNode(selectedUri, "dataset")}
          />
        ) : (
          <DatasetsList
            datasetDependencies={datasetDependencies}
            selectedDagId={selectedDagId || undefined}
            onSelectNode={onSelectNode}
          />
        )}
      </Box>
      <Box
        width={2}
        cursor="ew-resize"
        bg="gray.200"
        ref={resizeRef}
        zIndex={1}
        height={height}
      />
      <Box
        ref={graphRef}
        flex={1}
        height={height}
        borderColor="gray.200"
        borderWidth={1}
        position="relative"
      >
        {isLoading && <Spinner position="absolute" top="50%" left="50%" />}
        <Graph selectedNodeId={selectedNodeId} onSelectNode={onSelectNode} />
      </Box>
    </Flex>
  );
};

export default Datasets;
