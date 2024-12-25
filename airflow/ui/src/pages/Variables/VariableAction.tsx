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
// TODO: Disable lint will be removed once the implementation completed

/* eslint-disable no-alert */
import { IconButton } from "@chakra-ui/react";
import { FiEdit, FiEye, FiMoreVertical, FiTrash } from "react-icons/fi";

import { Menu } from "../../components/ui/Menu";

type VariableActionsProps = {
  readonly variableKey: string;
};

export const VariableActions = ({ variableKey }: VariableActionsProps) => (
  <Menu.Root>
    <Menu.Trigger asChild>
      <IconButton aria-label="Action menu" variant="ghost">
        <FiMoreVertical />
      </IconButton>
    </Menu.Trigger>

    <Menu.Content>
      <Menu.Item
        onClick={() =>
          alert(`To be implemented: Selected key is ${variableKey}`)
        }
        value="view variable"
      >
        <FiEye /> View
      </Menu.Item>
      <Menu.Item
        onClick={() =>
          alert(`To be implemented: Selected key is ${variableKey}`)
        }
        value="edit variable"
      >
        <FiEdit /> Edit
      </Menu.Item>
      <Menu.Item
        onClick={() =>
          alert(`To be implemented: Selected key is ${variableKey}`)
        }
        value="delete variable"
      >
        <FiTrash /> Delete
      </Menu.Item>
    </Menu.Content>
  </Menu.Root>
);
