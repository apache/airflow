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
import { IconButton, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";

import type { AssetResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";

import { CreateAssetEventModal } from "./CreateAssetEventModal";

type Props = {
  readonly asset?: AssetResponse;
};

export const CreateAssetEvent = ({ asset }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation("assets");

  return (
    <>
      <Tooltip content={translate("createEvent.button")}>
        <IconButton
          aria-label={translate("createEvent.button")}
          colorPalette="brand"
          disabled={asset === undefined}
          onClick={onOpen}
          size="md"
          variant="ghost"
        >
          <FiPlay />
        </IconButton>
      </Tooltip>

      {asset === undefined || !open ? undefined : (
        <CreateAssetEventModal asset={asset} onClose={onClose} open={open} />
      )}
    </>
  );
};
