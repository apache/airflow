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
import { Box } from "@chakra-ui/react";
import { useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";

import ActionButton from "../ui/ActionButton";
import TriggerDAGModal from "./TriggerDAGModal";

type Props = {
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly isPaused: boolean;
  readonly withText?: boolean;
};

const TriggerDAGButton: React.FC<Props> = ({ dagDisplayName, dagId, isPaused, withText = true }) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation("components");

  return (
    <Box>
      <ActionButton
        actionName={translate("triggerDag.title")}
        icon={<FiPlay />}
        onClick={onOpen}
        text={translate("triggerDag.button")}
        variant="outline"
        withText={withText}
      />

      <TriggerDAGModal
        dagDisplayName={dagDisplayName}
        dagId={dagId}
        isPaused={isPaused}
        onClose={onClose}
        open={open}
      />
    </Box>
  );
};

export default TriggerDAGButton;
