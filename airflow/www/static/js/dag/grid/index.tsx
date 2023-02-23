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
  IconButton,
} from '@chakra-ui/react';

import { MdDoubleArrow } from 'react-icons/md';

import { useGridData } from 'src/api';
import { getMetaValue } from 'src/utils';
import useOffsetHeight from 'src/utils/useOffsetHeight';

import renderTaskRows from './renderTaskRows';
import DagRuns from './dagRuns';

const dagId = getMetaValue('dag_id');

interface Props {
  isPanelOpen?: boolean;
  onPanelToggle?: () => void;
  hoveredTaskState?: string | null;
}

const Grid = ({
  isPanelOpen = false,
  onPanelToggle,
  hoveredTaskState,
}: Props) => {
  const scrollRef = useRef<HTMLDivElement>(null);
  const tableRef = useRef<HTMLTableSectionElement>(null);
  const gridRef = useRef<HTMLDivElement>(null);
  const offsetHeight = useOffsetHeight(scrollRef, undefined, 750);

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
      p={3}
      pt={0}
      mt={0}
      height="100%"
      ref={gridRef}
      position="relative"
    >
      <IconButton
        fontSize="2xl"
        variant="ghost"
        color="gray.400"
        size="sm"
        onClick={onPanelToggle}
        title={`${isPanelOpen ? 'Hide ' : 'Show '} Details Panel`}
        aria-label={isPanelOpen ? 'Show Details' : 'Hide Details'}
        icon={<MdDoubleArrow />}
        transform={!isPanelOpen ? 'rotateZ(180deg)' : undefined}
        transitionProperty="none"
        position="absolute"
        right={0}
        zIndex={2}
        top="30px"
      />
      <Box
        height="100%"
        maxHeight={offsetHeight}
        ref={scrollRef}
        overflow="auto"
        position="relative"
        pr={4}
        pb={4}
      >
        <Table pr="10px">
          <Thead>
            <DagRuns
              groups={groups}
              openGroupIds={openGroupIds}
              onToggleGroups={onToggleGroups}
            />
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
