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
import { Text, type TextProps } from "@chakra-ui/react";
import type { CSSProperties } from "react";
import { FiArrowUpRight, FiArrowDownRight } from "react-icons/fi";

import type { NodeResponse } from "openapi/requests/types.gen";

export type TaskNameProps = {
  readonly childCount?: number;
  readonly isGroup?: boolean;
  readonly isMapped?: boolean;
  readonly isOpen?: boolean;
  readonly isZoomedOut?: boolean;
  readonly label: string;
  readonly setupTeardownType?: NodeResponse["setup_teardown_type"];
} & TextProps;

const iconStyle: CSSProperties = {
  display: "inline",
  position: "relative",
  verticalAlign: "middle",
};

export const TaskName = ({
  childCount,
  isGroup = false,
  isMapped = false,
  isOpen = false,
  isZoomedOut,
  label,
  setupTeardownType,
  ...rest
}: TaskNameProps) => {
  if (isGroup) {
    return (
      <Text
        fontSize="md"
        fontWeight="bold"
        overflow="hidden"
        textOverflow="ellipsis"
        title={label}
        whiteSpace="nowrap"
        {...rest}
      >
        {label}
        {isMapped ? " [ ]" : undefined}
      </Text>
    );
  }

  return (
    <Text
      fontSize={isZoomedOut ? "lg" : "md"}
      fontWeight="bold"
      overflow="hidden"
      textOverflow="ellipsis"
      title={label}
      whiteSpace="nowrap"
      {...rest}
    >
      {label}
      {isMapped ? ` [${childCount ?? " "}]` : undefined}
      {setupTeardownType === "setup" && <FiArrowUpRight size={isZoomedOut ? 24 : 15} style={iconStyle} />}
      {setupTeardownType === "teardown" && (
        <FiArrowDownRight size={isZoomedOut ? 24 : 15} style={iconStyle} />
      )}
    </Text>
  );
};
