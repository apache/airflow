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
import { MdMoreHoriz } from "react-icons/md";

import type { DAGResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { Menu } from "src/components/ui";

import ActionButton from "../ui/ActionButton";
import RunBackfillButton from "./RunBackfillButton";

type Props = {
  readonly dag: DAGResponse | DAGWithLatestDagRunsResponse;
};

const MenuButton: React.FC<Props> = ({ dag }) => (
  <Menu.Root positioning={{ placement: "bottom" }}>
    <Menu.Trigger asChild>
      <ActionButton actionName="" icon={<MdMoreHoriz />} text="" />
    </Menu.Trigger>
    <Menu.Content>
      <Menu.Item value="Run Backfill">
        <RunBackfillButton dag={dag} />
      </Menu.Item>
    </Menu.Content>
  </Menu.Root>
);

export default MenuButton;
