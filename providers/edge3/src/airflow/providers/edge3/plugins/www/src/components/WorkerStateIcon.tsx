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
import type { EdgeWorkerState } from "openapi/requests/types.gen";
import type { IconBaseProps } from "react-icons";
import { BiSolidLeaf } from "react-icons/bi";
import { FiActivity, FiArrowDownRight, FiWatch } from "react-icons/fi";
import { HiOutlineWrench, HiOutlineWrenchScrewdriver } from "react-icons/hi2";
import { IoCloudOfflineOutline } from "react-icons/io5";
import { LuCircleDashed } from "react-icons/lu";
import { RiWifiOffLine } from "react-icons/ri";

type Props = {
  readonly state?: EdgeWorkerState | null;
} & IconBaseProps;

export const WorkerStateIcon = ({ state, ...rest }: Props) => {
  switch (state) {
    case "starting":
      return <FiWatch {...rest} />;
    case "running":
      return <FiActivity {...rest} />;
    case "idle":
      return <BiSolidLeaf {...rest} />;
    case "shutdown request":
    case "terminating":
      return <FiArrowDownRight {...rest} />;
    case "offline":
      return <IoCloudOfflineOutline {...rest} />;
    case "unknown":
      return <RiWifiOffLine {...rest} />;
    case "maintenance request":
    case "maintenance pending":
    case "maintenance exit":
      return <HiOutlineWrench {...rest} />;
    case "maintenance mode":
    case "offline maintenance":
      return <HiOutlineWrenchScrewdriver {...rest} />;
    default:
      return <LuCircleDashed {...rest} />;
  }
};
