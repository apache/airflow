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

/* global localStorage, ResizeObserver */

import React, { useState, useRef, useEffect } from 'react';
import {
  Table,
  Tbody,
  Box,
  Switch,
  FormControl,
  FormLabel,
  Spinner,
  Thead,
  Flex,
} from '@chakra-ui/react';

import { useGridData } from './api';
import renderTaskRows from './renderTaskRows';
import ResetRoot from './ResetRoot';
import DagRuns from './dagRuns';
import { useAutoRefresh } from './context/autorefresh';
import ToggleGroups from './ToggleGroups';
import FilterBar from './FilterBar';
import LegendRow from './LegendRow';

const dagId = getMetaValue('dag_id');

const Grid = ({ isPanelOpen }) => {
  const scrollRef = useRef();
  const tableRef = useRef();

  const { data: { groups, dagRuns } } = useGridData();
  const dagRunIds = dagRuns.map((dr) => dr.runId);

  const { isRefreshOn, toggleRefresh, isPaused } = useAutoRefresh();

  const openGroupsKey = `${dagId}/open-groups`;
  const storedGroups = JSON.parse(localStorage.getItem(openGroupsKey)) || [];
  const [openGroupIds, setOpenGroupIds] = useState(storedGroups);


  const onToggleGroups = (groupIds) => {
    localStorage.setItem(openGroupsKey, JSON.stringify(groupIds));
    setOpenGroupIds(groupIds);
  };

  const scrollOnResize = new ResizeObserver(() => {
    const runsContainer = scrollRef.current;
    // Set scroll to top right if it is scrollable
    if (runsContainer && runsContainer.scrollWidth > runsContainer.clientWidth) {
      runsContainer.scrollBy(tableRef.current.offsetWidth, 0);
    }
  });

  useEffect(() => {
    if (tableRef && tableRef.current) {
      const table = tableRef.current;

      scrollOnResize.observe(table);
      return () => {
        scrollOnResize.unobserve(table);
      };
    }
    return () => {};
  }, [tableRef, scrollOnResize]);

  return (
    <Box
      position="relative"
      mt={2}
      m="12px"
      overflow="auto"
      ref={scrollRef}
      flexGrow={1}
      minWidth={isPanelOpen && '300px'}
    >
      <Flex alignItems="center">
        <FormControl display="flex" width="auto" mr={2}>
          {isRefreshOn && <Spinner color="blue.500" speed="1s" mr="4px" />}
          <FormLabel htmlFor="auto-refresh" mb={0} fontWeight="normal">
            Auto-refresh
          </FormLabel>
          <Switch
            id="auto-refresh"
            onChange={() => toggleRefresh(true)}
            isDisabled={isPaused}
            isChecked={isRefreshOn}
            size="lg"
            title={isPaused ? 'Autorefresh is disabled while the DAG is paused' : ''}
          />
        </FormControl>
        <ToggleGroups
          groups={groups}
          openGroupIds={openGroupIds}
          onToggleGroups={onToggleGroups}
        />
        <ResetRoot />
      </Flex>
      <Table>
        <Thead display="block" pr="10px" position="sticky" top={0} zIndex={2} bg="white">
          <DagRuns />
        </Thead>
        {/* TODO: remove hardcoded values. 665px is roughly the total heade+footer height */}
        <Tbody display="block" width="100%" maxHeight="calc(100vh - 665px)" minHeight="500px" ref={tableRef} pr="10px">
          {renderTaskRows({
            task: groups, dagRunIds, openGroupIds, onToggleGroups,
          })}
        </Tbody>
      </Table>
    </Box>
  );
};

export default Grid;
