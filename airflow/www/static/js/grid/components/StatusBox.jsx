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
  Box,
  Tooltip,
  useTheme,
} from '@chakra-ui/react';

import InstanceTooltip from './InstanceTooltip';
import { useContainerRef } from '../context/containerRef';
import useFilters from '../utils/useFilters';

export const boxSize = 10;
export const boxSizePx = `${boxSize}px`;

export const SimpleStatus = ({ state, ...rest }) => (
  <Box
    width={boxSizePx}
    height={boxSizePx}
    backgroundColor={stateColors[state] || 'white'}
    borderRadius="2px"
    borderWidth={state ? 0 : 1}
    {...rest}
  />
);

const StatusBox = ({
  group, instance, onSelect,
}) => {
  const containerRef = useContainerRef();
  const { runId, taskId } = instance;
  const { colors } = useTheme();
  const hoverBlue = `${colors.blue[100]}50`;
  const { filters } = useFilters();

  // Fetch the corresponding column element and set its background color when hovering
  const onMouseEnter = () => {
    [...containerRef.current.getElementsByClassName(`js-${runId}`)]
      .forEach((e) => {
        // Don't apply hover if it is already selected
        if (e.getAttribute('data-selected') === 'false') e.style.backgroundColor = hoverBlue;
      });
  };
  const onMouseLeave = () => {
    [...containerRef.current.getElementsByClassName(`js-${runId}`)]
      .forEach((e) => { e.style.backgroundColor = null; });
  };

  const onClick = () => {
    onMouseLeave();
    onSelect({ taskId, runId });
  };

  return (
    <Tooltip
      label={<InstanceTooltip instance={instance} group={group} />}
      portalProps={{ containerRef }}
      hasArrow
      placement="top"
      openDelay={400}
    >
      <Box>
        <SimpleStatus
          state={instance.state}
          onClick={onClick}
          cursor="pointer"
          data-testid="task-instance"
          zIndex={1}
          onMouseEnter={onMouseEnter}
          onMouseLeave={onMouseLeave}
          opacity={(filters.taskState && filters.taskState !== instance.state) ? 0.30 : 1}
        />
      </Box>
    </Tooltip>
  );
};

// The default equality function is a shallow comparison and json objects will return false
// This custom compare function allows us to do a deeper comparison
const compareProps = (
  prevProps,
  nextProps,
) => (
  isEqual(prevProps.group, nextProps.group)
  && isEqual(prevProps.instance, nextProps.instance)
);

export default React.memo(StatusBox, compareProps);
