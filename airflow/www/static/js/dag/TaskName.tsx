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

import React, { CSSProperties } from "react";
import { Text, TextProps, useTheme, chakra } from "@chakra-ui/react";
import { FiChevronUp, FiArrowUpRight, FiArrowDownRight } from "react-icons/fi";

interface Props extends TextProps {
  isGroup?: boolean;
  isMapped?: boolean;
  isOpen?: boolean;
  label: string;
  id?: string;
  setupTeardownType?: string;
  isZoomedOut?: boolean;
}

const TaskName = ({
  isGroup = false,
  isMapped = false,
  isOpen = false,
  label,
  id,
  setupTeardownType,
  isZoomedOut,
  onClick,
  ...rest
}: Props) => {
  const { colors } = useTheme();
  const iconStyle: CSSProperties = {
    display: "inline",
    position: "relative",
    verticalAlign: "middle",
  };
  return (
    <Text
      cursor="pointer"
      data-testid={id}
      color={colors.gray[800]}
      fontSize={isZoomedOut ? 24 : undefined}
      {...rest}
    >
      <chakra.span onClick={onClick}>
        {label}
        {isMapped && " [ ]"}
        {isGroup && (
          <FiChevronUp
            size={isZoomedOut ? 24 : 15}
            strokeWidth={3}
            style={{
              transition: "transform 0.5s",
              transform: `rotate(${isOpen ? 0 : 180}deg)`,
              ...iconStyle,
            }}
          />
        )}
        {setupTeardownType === "setup" && (
          <FiArrowUpRight size={isZoomedOut ? 24 : 15} style={iconStyle} />
        )}
        {setupTeardownType === "teardown" && (
          <FiArrowDownRight size={isZoomedOut ? 24 : 15} style={iconStyle} />
        )}
      </chakra.span>
    </Text>
  );
};

// Only rerender the component if props change
const MemoizedTaskName = React.memo(TaskName);

export default MemoizedTaskName;
