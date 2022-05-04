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

import React, { useRef, useEffect } from 'react';
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
  useDisclosure,
  Button,
  Divider,
} from '@chakra-ui/react';

import { useGridData } from './api';
import renderTaskRows from './renderTaskRows';
import ResetRoot from './ResetRoot';
import DagRuns from './dagRuns';
import Details from './details';
import useSelection from './utils/useSelection';
import { useAutoRefresh } from './context/autorefresh';
import ToggleGroups from './ToggleGroups';
import FilterBar from './FilterBar';
import LegendRow from './LegendRow';

const sidePanelKey = 'hideSidePanel';

const Grid = () => {
  const scrollRef = useRef();
  const tableRef = useRef();

  const { data: { groups, dagRuns } } = useGridData();
  const dagRunIds = dagRuns.map((dr) => dr.runId);

  const { isRefreshOn, toggleRefresh, isPaused } = useAutoRefresh();
  const isPanelOpen = localStorage.getItem(sidePanelKey) !== 'true';
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: isPanelOpen });

  const { clearSelection } = useSelection();
  const toggleSidePanel = () => {
    if (!isOpen) {
      localStorage.setItem(sidePanelKey, false);
    } else {
      clearSelection();
      localStorage.setItem(sidePanelKey, true);
    }
    onToggle();
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
    <Box mt={3}>
      <FilterBar />
      <LegendRow />
      <Divider mb={5} borderBottomWidth={2} />
      <Flex flexGrow={1} justifyContent="space-between" alignItems="center">
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
          <ToggleGroups groups={groups} />
          <ResetRoot />
        </Flex>
        <Button
          onClick={toggleSidePanel}
          aria-label={isOpen ? 'Show Details' : 'Hide Details'}
          variant={isOpen ? 'solid' : 'outline'}
        >
          {isOpen ? 'Hide ' : 'Show '}
          Details Panel
        </Button>
      </Flex>
      <Flex flexDirection="row" justifyContent="space-between">
        <Box
          position="relative"
          mt={2}
          m="12px"
          overflow="auto"
          ref={scrollRef}
          flexGrow={1}
          minWidth={isOpen && '300px'}
        >
          <Table>
            <Thead display="block" pr="10px" position="sticky" top={0} zIndex={2} bg="white">
              <DagRuns />
            </Thead>
            {/* TODO: remove hardcoded values. 665px is roughly the total header+footer height */}
            <Tbody display="block" width="100%" maxHeight="calc(100vh - 665px)" minHeight="500px" ref={tableRef} pr="10px">
              {renderTaskRows({
                task: groups, dagRunIds,
              })}
            </Tbody>
          </Table>
        </Box>
        {isOpen && (
          <Details />
        )}
      </Flex>
    </Box>
  );
};

export default Grid;
