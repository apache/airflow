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

import type { AssetResponse } from "openapi/requests/types.gen";
import ActionButton from "src/components/ui/ActionButton";

import { CreateAssetEventModal } from "./CreateAssetEventModal";

type Props = {
  readonly asset?: AssetResponse;
  readonly withText?: boolean;
};

export const CreateAssetEvent = ({ asset, withText = true }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation("assets");

  return (
    <Box>
      <ActionButton
        actionName={translate("createEvent.button")}
        colorPalette="brand"
        disabled={asset === undefined}
        icon={<FiPlay />}
        onClick={onOpen}
        text={translate("createEvent.button")}
        variant="solid"
        withText={withText}
      />

      {asset === undefined || !open ? undefined : (
        <CreateAssetEventModal asset={asset} onClose={onClose} open={open} />
      )}
    </Box>
  );
};
