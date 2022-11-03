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
  Thead,
  Flex,
  IconButton,
  useDimensions,
} from '@chakra-ui/react';

import { MdReadMore } from 'react-icons/md';

import { useGridData } from 'src/api';
import { getMetaValue } from 'src/utils';
import AutoRefresh from 'src/components/AutoRefresh';

import renderTaskRows from './renderTaskRows';
import ResetRoot from './ResetRoot';
import DagRuns from './dagRuns';
import ToggleGroups from './ToggleGroups';

const dagId = getMetaValue('dag_id');

interface Props {
  isPanelOpen?: boolean;
  onPanelToggle?: () => void;
  hoveredTaskState?: string | null;
}

const Grid = ({ isPanelOpen = false, onPanelToggle, hoveredTaskState }: Props) => {
  const scrollRef = useRef<HTMLDivElement>(null);
  const tableRef = useRef<HTMLTableSectionElement>(null);
  const buttonsRef = useRef<HTMLDivElement>(null);
  const dimensions = useDimensions(buttonsRef);

  const { data: { groups, dagRuns } } = useGridData();
  const dagRunIds = dagRuns.map((dr) => dr.runId);

  const openGroupsKey = `${dagId}/open-groups`;
  const storedGroups = JSON.parse(localStorage.getItem(openGroupsKey) || '[]');
  const [openGroupIds, setOpenGroupIds] = useState(storedGroups);

  const onToggleGroups = (groupIds: string[]) => {
    localStorage.setItem(openGroupsKey, JSON.stringify(groupIds));
    setOpenGroupIds(groupIds);
  };

  useEffect(() => {
    const scrollOnResize = new ResizeObserver(() => {
      const runsContainer = scrollRef.current;
      // Set scroll to top right if it is scrollable
      if (
        tableRef?.current
        && runsContainer
        && runsContainer.scrollWidth > runsContainer.clientWidth
      ) {
        runsContainer.scrollBy(tableRef.current.offsetWidth, 0);
      }
    });

    if (tableRef && tableRef.current) {
      const table = tableRef.current;

      scrollOnResize.observe(table);
      return () => {
        scrollOnResize.unobserve(table);
      };
    }
    return () => {};
  }, [tableRef, isPanelOpen]);

  return (
    <Box
      m={3}
      mt={0}
      height="100%"
    >
      <Flex
        alignItems="center"
        justifyContent="space-between"
        p={1}
        pb={2}
        backgroundColor="white"
        ref={buttonsRef}
      >
        <Flex alignItems="center">
          <AutoRefresh />
          <ToggleGroups
            groups={groups}
            openGroupIds={openGroupIds}
            onToggleGroups={onToggleGroups}
          />
          <ResetRoot />
        </Flex>
        <IconButton
          fontSize="2xl"
          onClick={onPanelToggle}
          title={`${isPanelOpen ? 'Hide ' : 'Show '} Details Panel`}
          aria-label={isPanelOpen ? 'Show Details' : 'Hide Details'}
          icon={<MdReadMore />}
          transform={!isPanelOpen ? 'rotateZ(180deg)' : undefined}
          transitionProperty="none"
        />
      </Flex>
      <Box
        height={`calc(100% - ${dimensions?.borderBox.height || 0}px)`}
        ref={scrollRef}
        overflow="auto"
        position="relative"
        pr={4}
      >
        <Table pr="10px">
          <Thead>
            <DagRuns />
          </Thead>
          <Tbody ref={tableRef}>
            {renderTaskRows({
              task: groups, dagRunIds, openGroupIds, onToggleGroups, hoveredTaskState,
            })}
          </Tbody>
        </Table>
      </Box>
    </Box>
  );
};

export default Grid;
