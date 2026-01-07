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
import { Heading, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiEdit } from "react-icons/fi";

import type { ConnectionResponse } from "openapi/requests/types.gen";
import { Dialog } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { useEditConnection } from "src/queries/useEditConnection";

import ConnectionForm from "./ConnectionForm";
import type { ConnectionBody } from "./Connections";

type Props = {
  readonly connection: ConnectionResponse;
  readonly disabled: boolean;
};

const EditConnectionButton = ({ connection, disabled }: Props) => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const initialConnectionValue: ConnectionBody = {
    conn_type: connection.conn_type,
    connection_id: connection.connection_id,
    description: connection.description ?? "",
    extra: connection.extra === "" || connection.extra === null ? "{}" : connection.extra,
    host: connection.host ?? "",
    login: connection.login ?? "",
    password: connection.password ?? "",
    port: connection.port?.toString() ?? "",
    schema: connection.schema ?? "",
    team_name: connection.team_name ?? "",
  };
  const { editConnection, error, isPending, setError } = useEditConnection(initialConnectionValue, {
    onSuccessConfirm: onClose,
  });

  const handleClose = () => {
    setError(undefined);
    onClose();
  };

  return (
    <>
      <ActionButton
        actionName={translate("connections.edit")}
        disabled={disabled}
        icon={<FiEdit />}
        onClick={() => {
          onOpen();
        }}
        text={translate("connections.edit")}
        withText={false}
      />

      <Dialog.Root onOpenChange={handleClose} open={open} size="xl">
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading size="xl">{translate("connections.edit")}</Heading>
          </Dialog.Header>

          <Dialog.CloseTrigger />

          <Dialog.Body>
            <ConnectionForm
              error={error}
              initialConnection={initialConnectionValue}
              isEditMode
              isPending={isPending}
              mutateConnection={editConnection}
            />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};

export default EditConnectionButton;
