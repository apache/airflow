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
import { isEqual } from "lodash";
import { Box, useTheme, BoxProps } from "@chakra-ui/react";
import { MdRefresh } from "react-icons/md";

import { useContainerRef } from "src/context/containerRef";
import type { Task, TaskInstance, TaskState } from "src/types";
import type { SelectionProps } from "src/dag/useSelection";
import { getStatusBackgroundColor, hoverDelay } from "src/utils";
import Tooltip from "src/components/Tooltip";

import InstanceTooltip from "src/components/InstanceTooltip";

export const boxSize = 10;
export const boxSizePx = `${boxSize}px`;

interface StatusWithNotesProps extends BoxProps {
  state: TaskState;
  tryNumber?: number;
  containsNotes?: boolean;
}

export const StatusWithNotes = ({
  state,
  containsNotes,
  tryNumber,
  ...rest
}: StatusWithNotesProps) => {
  const color = state && stateColors[state] ? stateColors[state] : "white";
  return (
    <Box
      width={boxSizePx}
      height={boxSizePx}
      background={getStatusBackgroundColor(color, !!containsNotes)}
      borderRadius="2px"
      borderWidth={state ? 0 : 1}
      display="flex"
      alignItems="center"
      justifyContent="center"
      {...rest}
    >
      {tryNumber !== undefined && tryNumber > 1 && <MdRefresh color="white" />}
    </Box>
  );
};

interface SimpleStatusProps extends BoxProps {
  state: TaskState | undefined;
}
export const SimpleStatus = ({ state, ...rest }: SimpleStatusProps) => (
  <Box
    width={boxSizePx}
    height={boxSizePx}
    background={state && stateColors[state] ? stateColors[state] : "white"}
    borderRadius="2px"
    borderWidth={state ? 0 : 1}
    display="flex"
    flexDirection="column"
    justifyContent="center"
    alignItems="center"
    style={{ marginTop: "auto", marginBottom: "auto" }}
    {...rest}
  />
);

interface Props {
  group: Task;
  instance: TaskInstance;
  onSelect: (selection: SelectionProps) => void;
  isActive: boolean;
  containsNotes?: boolean;
}

const StatusBox = ({
  group,
  instance,
  onSelect,
  isActive,
  containsNotes = false,
}: Props) => {
  const containerRef = useContainerRef();
  const { runId, taskId } = instance;
  const { colors } = useTheme();
  const hoverBlue = `${colors.blue[100]}50`;

  // Fetch the corresponding column element and set its background color when hovering
  const onMouseEnter = () => {
    if (containerRef && containerRef.current) {
      (
        [
          ...containerRef.current.getElementsByClassName(`js-${runId}`),
        ] as HTMLElement[]
      ).forEach((e) => {
        // Don't apply hover if it is already selected
        if (e.getAttribute("data-selected") === "false")
          e.style.backgroundColor = hoverBlue;
      });
    }
  };
  const onMouseLeave = () => {
    if (containerRef && containerRef.current) {
      (
        [
          ...containerRef.current.getElementsByClassName(`js-${runId}`),
        ] as HTMLElement[]
      ).forEach((e) => {
        e.style.backgroundColor = "";
      });
    }
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
      openDelay={hoverDelay}
    >
      <Box>
        <StatusWithNotes
          state={instance.state}
          containsNotes={containsNotes}
          tryNumber={instance.tryNumber}
          onClick={onClick}
          cursor="pointer"
          data-testid="task-instance"
          zIndex={1}
          onMouseEnter={onMouseEnter}
          onMouseLeave={onMouseLeave}
          opacity={isActive ? 1 : 0.3}
          transition="opacity 0.2s"
        />
      </Box>
    </Tooltip>
  );
};

// The default equality function is a shallow comparison and json objects will return false
// This custom compare function allows us to do a deeper comparison
const compareProps = (prevProps: Props, nextProps: Props) =>
  isEqual(prevProps.group, nextProps.group) &&
  isEqual(prevProps.instance, nextProps.instance) &&
  isEqual(prevProps.isActive, nextProps.isActive);

export default React.memo(StatusBox, compareProps);
