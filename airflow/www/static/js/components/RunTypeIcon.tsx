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
import { MdPlayArrow, MdOutlineSchedule } from "react-icons/md";
import { RiArrowGoBackFill } from "react-icons/ri";
import { HiDatabase } from "react-icons/hi";

import type { IconBaseProps } from "react-icons";
import type { DagRun } from "src/types";

interface Props extends IconBaseProps {
  runType: DagRun["runType"];
}

const iconStyle = {
  display: "inline",
  verticalAlign: "bottom",
};

const DagRunTypeIcon = ({ runType, ...rest }: Props) => {
  switch (runType) {
    case "manual":
      return <MdPlayArrow style={iconStyle} {...rest} />;
    case "backfill":
      return <RiArrowGoBackFill style={iconStyle} {...rest} />;
    case "scheduled":
      return <MdOutlineSchedule style={iconStyle} {...rest} />;
    case "dataset_triggered":
      return <HiDatabase style={iconStyle} {...rest} />;
    default:
      return null;
  }
};

export default DagRunTypeIcon;
