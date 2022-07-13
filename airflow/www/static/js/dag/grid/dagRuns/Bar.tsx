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

/* global stateColors */

import React from 'react';
import { isEqual } from 'lodash';
import {
  Flex,
  Box,
  Tooltip,
  Text,
  VStack,
  useTheme,
} from '@chakra-ui/react';
import { MdPlayArrow } from 'react-icons/md';
import { RiArrowGoBackFill } from 'react-icons/ri';

import { useContainerRef } from 'src/context/containerRef';
import Time from 'src/components/Time';
import type { SelectionProps } from 'src/dag/useSelection';
import { hoverDelay } from 'src/utils';

import DagRunTooltip from './Tooltip';
import type { RunWithDuration } from '.';

const BAR_HEIGHT = 100;

interface Props {
  run: RunWithDuration
  max: number;
  index: number;
  totalRuns: number;
  isSelected: boolean;
  onSelect: (props: SelectionProps) => void;
}

const DagRunBar = ({
  run, max, index, totalRuns, isSelected, onSelect,
}: Props) => {
  const containerRef = useContainerRef();
  const { colors } = useTheme();
  const hoverBlue = `${colors.blue[100]}50`;

  // Fetch the corresponding column element and set its background color when hovering
  const onMouseEnter = () => {
    if (!isSelected) {
      const els = Array.from(containerRef?.current?.getElementsByClassName(`js-${run.runId}`) as HTMLCollectionOf<HTMLElement>);
      els.forEach((e) => { e.style.backgroundColor = hoverBlue; });
    }
  };
  const onMouseLeave = () => {
    const els = Array.from(containerRef?.current?.getElementsByClassName(`js-${run.runId}`) as HTMLCollectionOf<HTMLElement>);
    els.forEach((e) => { e.style.backgroundColor = ''; });
  };

  // show the tick on the 4th DagRun and then every 10th tick afterwards
  const inverseIndex = totalRuns - index;
  const shouldShowTick = inverseIndex === 4
    || (inverseIndex > 4 && (inverseIndex - 4) % 10 === 0);

  return (
    <Box
      className={`js-${run.runId}`}
      data-selected={isSelected}
      bg={isSelected ? 'blue.100' : undefined}
      transition="background-color 0.2s"
      px="1px"
      pb="2px"
      position="relative"
    >
      <Flex
        height={BAR_HEIGHT}
        alignItems="flex-end"
        justifyContent="center"
        px="2px"
        cursor="pointer"
        width="14px"
        zIndex={1}
        onMouseEnter={onMouseEnter}
        onMouseLeave={onMouseLeave}
        onClick={() => onSelect({ runId: run.runId })}
      >
        <Tooltip
          label={<DagRunTooltip dagRun={run} />}
          hasArrow
          portalProps={{ containerRef }}
          placement="top"
          openDelay={hoverDelay}
        >
          <Flex
            width="10px"
            height={`${(run.duration / max) * BAR_HEIGHT}px`}
            minHeight="14px"
            backgroundColor={stateColors[run.state]}
            borderRadius={2}
            cursor="pointer"
            pb="2px"
            direction="column"
            justifyContent="flex-end"
            alignItems="center"
            px="1px"
            zIndex={1}
            data-testid="run"
          >
            {run.runType === 'manual' && <MdPlayArrow size="8px" color="white" data-testid="manual-run" />}
            {run.runType === 'backfill' && <RiArrowGoBackFill size="8px" color="white" data-testid="backfill-run" />}
          </Flex>
        </Tooltip>
      </Flex>
      {shouldShowTick && (
      <VStack position="absolute" top="0" left="8px" spacing={0} zIndex={0} width={0}>
        <Text fontSize="sm" color="gray.400" whiteSpace="nowrap" transform="rotate(-30deg) translateX(28px)" mt="-23px !important">
          <Time dateTime={run.executionDate} format="MMM DD, HH:mm" />
        </Text>
        <Box borderLeftWidth={1} opacity={0.7} height="100px" zIndex={0} />
      </VStack>
      )}
    </Box>
  );
};

// The default equality function is a shallow comparison and json objects will return false
// This custom compare function allows us to do a deeper comparison
const compareProps = (
  prevProps: Props,
  nextProps: Props,
) => (
  isEqual(prevProps.run, nextProps.run)
  && prevProps.max === nextProps.max
  && prevProps.index === nextProps.index
  && prevProps.totalRuns === nextProps.totalRuns
  && prevProps.isSelected === nextProps.isSelected
);

export default React.memo(DagRunBar, compareProps);
