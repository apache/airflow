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
import { Badge, type BadgeProps, HoverCard, List, Text } from "@chakra-ui/react";
import * as React from "react";
import TimeAgo from "react-timeago";

import { LuCheck } from "react-icons/lu";
import { MdErrorOutline } from "react-icons/md";
import { PiQuestion, PiWarning } from "react-icons/pi";

const status2Color = (status: number | undefined) => {
  // logging.NOTSET == 0, keep 10 as boundary between NOTSET and INFO
  if (status === undefined || status <= 10) {
    return "gray";
  }
  // logging.INFO == 20, keep 25 as boundary between INFO and WARNING
  if (status <= 25) {
    return "green";
  }
  // logging.WARNING == 30, keep 35 as boundary between WARNING and ERROR
  if (status <= 35) {
    return "yellow";
  }
  // all other assume is like logging.ERROR == 40 or higher
  return "red";
};

const status2Text = (status: number | undefined) => {
  // same levels as above in status2Color
  if (status === undefined || status <= 10) {
    return "Unknown";
  }
  // logging.INFO == 20, keep 25 as boundary between INFO and WARNING
  if (status <= 25) {
    return "Healthy";
  }
  // logging.WARNING == 30, keep 35 as boundary between WARNING and ERROR
  if (status <= 35) {
    return "Warning";
  }
  // all other assume is like logging.ERROR == 40 or higher
  return "Error";
};

const capitalize = (s: string) => s.charAt(0).toUpperCase() + s.slice(1).replaceAll("_", " ");

const isDate = (value: string | number): boolean => {
  if (typeof value === "number") {
    return false; // numbers are not considered dates in this context
  }
  // Do not attempt to parse version strings that may look like dates but are not
  if (/^\d+\.\d+\.\d+.*/.test(value)) {
    return false;
  }
  // Check if the value is a string that can be parsed into a date
  const date = Date.parse(value);
  return !isNaN(date);
};

type IconProps = {
  readonly status: number | undefined;
};

const WorkerSysinfoIcon = ({ status, ...rest }: IconProps) => {
  // same levels as above in status2Color
  if (status === undefined || status <= 10) {
    return <PiQuestion {...rest} />;
  }
  if (status <= 25) {
    return <LuCheck {...rest} />;
  }
  if (status <= 35) {
    return <PiWarning {...rest} />;
  }
  return <MdErrorOutline {...rest} />;
};

export type Props = {
  readonly sysinfo: { [key: string]: string | number; }
  readonly first_online: string | null | undefined;
  readonly last_heartbeat: string | null | undefined;
} & BadgeProps;

export const WorkerSysinfoBadge = React.forwardRef<HTMLDivElement, Props>(
  ({ children, sysinfo, first_online, last_heartbeat, ...rest }, ref) => (
    <HoverCard.Root size={"lg"}>
      <HoverCard.Trigger>
        <Badge
          borderRadius="full"
          colorPalette={status2Color(sysinfo.status as number | undefined)}
          fontSize="sm"
          px={children === undefined ? 1 : 2}
          py={1}
          ref={ref}
          variant="solid"
          {...rest}
        >
          <WorkerSysinfoIcon status={sysinfo.status as number | undefined} />
          <Text ml={1} mr={2}>
            {sysinfo.status_text ?? status2Text(sysinfo.status as number | undefined)}
          </Text>
          {children}
        </Badge>
      </HoverCard.Trigger>
      <HoverCard.Positioner>
        <HoverCard.Content>
          <HoverCard.Arrow />
          {sysinfo ? (
            <List.Root>
              <List.Item>First online: {first_online ? <TimeAgo date={first_online} live={false} /> : "N/A"}</List.Item>
              <List.Item>Last heartbeat: {last_heartbeat ? <TimeAgo date={last_heartbeat} live={false} /> : "N/A"}</List.Item>
              {Object.entries(sysinfo).filter(([key]) => key !== "status" && key !== "status_text").map(([key, value]) => (
                <List.Item key={key}>
                  {capitalize(key)}: {isDate(value) ? <TimeAgo date={value + "Z"} live={false} /> : value}
                </List.Item>
              ))}
            </List.Root>
          ) : (
            "N/A"
          )}
        </HoverCard.Content>
      </HoverCard.Positioner>
    </HoverCard.Root>
  ),
);
