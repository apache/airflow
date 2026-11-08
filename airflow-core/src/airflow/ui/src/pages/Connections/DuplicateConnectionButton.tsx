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
import { useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiCopy } from "react-icons/fi";

import type { ConnectionResponse } from "openapi/requests/types.gen";

import { Alert, IconButton, Modal } from "src/system-components";

import { useAddConnection } from "src/queries/useAddConnection";

import ConnectionForm from "./ConnectionForm";
import type { ConnectionBody } from "./Connections";

// The API returns sensitive values as this placeholder. Editing merges it back into the stored secret,
// but a new connection has no stored secret, so the placeholder must not be submitted as a value.
const REDACTED = "***";

type Props = {
  readonly connection: ConnectionResponse;
  readonly disabled: boolean;
};

const DuplicateConnectionButton = ({ connection, disabled }: Props) => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const { addConnection, error, isPending } = useAddConnection({ onSuccessConfirm: onClose });

  let hasRedactedValues = connection.password === REDACTED;
  const extra =
    connection.extra === "" || connection.extra === null
      ? "{}"
      : JSON.stringify(
          JSON.parse(connection.extra, (_key, value: unknown) => {
            if (value === REDACTED) {
              hasRedactedValues = true;

              return undefined;
            }

            return value;
          }),
        );

  const initialConnection: ConnectionBody = {
    conn_type: connection.conn_type,
    connection_id: "",
    description: connection.description ?? "",
    extra,
    host: connection.host ?? "",
    login: connection.login ?? "",
    password: connection.password === REDACTED ? "" : (connection.password ?? ""),
    port: connection.port?.toString() ?? "",
    schema: connection.schema ?? "",
    team_name: connection.team_name ?? "",
  };

  return (
    <>
      <IconButton disabled={disabled} label={translate("connections.duplicate")} onClick={onOpen}>
        <FiCopy />
      </IconButton>
      <Modal
        lazyMount
        onOpenChange={onClose}
        open={open}
        title={translate("connections.duplicate")}
        unmountOnExit
      >
        {hasRedactedValues ? (
          <Alert status="info">{translate("connections.form.helperTextForDuplicatedRedactedFields")}</Alert>
        ) : undefined}
        <ConnectionForm
          error={error}
          initialConnection={initialConnection}
          isPending={isPending}
          mutateConnection={addConnection}
        />
      </Modal>
    </>
  );
};

export default DuplicateConnectionButton;
