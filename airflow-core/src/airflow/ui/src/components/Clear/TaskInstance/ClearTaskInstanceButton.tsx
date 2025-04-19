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
import { Box, useDisclosure } from "@chakra-ui/react";
import { CgRedo } from "react-icons/cg";

import type { TaskActionProps } from "src/components/MarkAs/utils";
import ActionButton from "src/components/ui/ActionButton";

import { ClearTaskInstanceDialog } from "./ClearTaskInstanceDialog";

type Props = {
  readonly taskActionProps: TaskActionProps;
  readonly withText?: boolean;
};

const ClearTaskInstanceButton = ({ taskActionProps, withText = true }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();

  return (
    <Box>
      <ActionButton
        actionName="Clear Task Instance"
        icon={<CgRedo />}
        onClick={onOpen}
        text="Clear Task Instance"
        withText={withText}
      />

      {open ? (
        <ClearTaskInstanceDialog onClose={onClose} open={open} taskActionProps={taskActionProps} />
      ) : undefined}
    </Box>
  );
};

export default ClearTaskInstanceButton;
