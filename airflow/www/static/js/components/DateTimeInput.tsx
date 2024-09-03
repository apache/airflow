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

/* global moment */

import React from "react";
import { Flex, Input, InputProps } from "@chakra-ui/react";

const DateTimeInput = (props: InputProps) => {
  // Firefox and safari do not support datetime-local so we need to put a date and time input next to each other
  const userAgent = navigator.userAgent.toLowerCase();
  const unsupportedBrowser =
    userAgent.includes("firefox") || userAgent.includes("safari");

  if (!unsupportedBrowser) return <Input type="datetime-local" {...props} />;

  const { value, onChange, ...rest } = props;
  // @ts-ignore
  let datetime = moment(value as string);
  // @ts-ignore
  datetime = datetime.isValid() ? datetime : moment();
  // @ts-ignore
  const date = moment(datetime).format("YYYY-MM-DD");
  // @ts-ignore
  const time = moment(datetime).format("HH:mm:ss");
  return (
    <Flex>
      <Input
        type="date"
        value={date}
        onChange={(e) => {
          if (onChange)
            onChange({
              ...e,
              target: {
                ...e.target,
                value: `${e.target.value}T${time}`,
              },
            });
        }}
        {...rest}
        borderRightWidth={0}
        borderRightRadius={0}
        paddingInlineEnd={0}
      />
      <Input
        type="time"
        value={time}
        onChange={(e) => {
          if (onChange)
            onChange({
              ...e,
              target: {
                ...e.target,
                value: `${date}T${e.target.value}`,
              },
            });
        }}
        {...rest}
        borderLeftWidth={0}
        borderLeftRadius={0}
        paddingInlineStart={0}
      />
    </Flex>
  );
};

export default DateTimeInput;
