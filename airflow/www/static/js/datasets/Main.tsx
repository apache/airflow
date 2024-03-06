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
import { Flex, Box } from "@chakra-ui/react";

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

  const datasetUriSearch = decodeURIComponent(
    searchParams.get(DATASET_URI_PARAM) || ""
  );
  const filteredDagIds = searchParams
    .getAll(DAG_ID_PARAM)
    .map((param) => decodeURIComponent(param));

  // We need to load in the raw dependencies in order to generate the list of dagIds
  const { data: datasetDependencies } = useDatasetDependencies();

  const onBack = () => {
    searchParams.delete(DATASET_URI_PARAM);
    setSearchParams(searchParams);
  };

  const onSelect = (datasetUri: string) => {
    searchParams.set(DATASET_URI_PARAM, encodeURIComponent(datasetUri));
    setSearchParams(searchParams);
  };

  const onFilterDags = (dagIds: string[]) => {
    const params = new URLSearchParams(
      dagIds.map((dagId) => [DAG_ID_PARAM, dagId])
    );
    if (datasetUriSearch) {
      params.append(DATASET_URI_PARAM, encodeURIComponent(datasetUriSearch));
    }
    setSearchParams(params);
  };

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

  return (
    <Flex
      alignItems="flex-start"
      justifyContent="space-between"
      ref={contentRef}
    >
      <Box
        minWidth={minPanelWidth}
        height={height}
        overflowY="auto"
        ref={listRef}
        mr={3}
      >
        {datasetUriSearch ? (
          <DatasetDetails uri={datasetUriSearch} onBack={onBack} />
        ) : (
          <DatasetsList
            onSelect={onSelect}
            datasetDependencies={datasetDependencies}
            filteredDagIds={filteredDagIds}
            onFilterDags={onFilterDags}
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
      >
        <Graph
          selectedUri={datasetUriSearch}
          onSelect={onSelect}
          filteredDagIds={filteredDagIds}
          onFilterDags={onFilterDags}
        />
      </Box>
    </Flex>
  );
};

export default Datasets;
