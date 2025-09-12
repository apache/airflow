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
import { FiTrash2 } from "react-icons/fi";

import DeleteDialog from "src/components/DeleteDialog";
import ActionButton from "src/components/ui/ActionButton";
import { useDeletePool } from "src/queries/useDeletePool";

type Props = {
  readonly poolName: string;
  readonly withText?: boolean;
};

const DeletePoolButton = ({ poolName, withText = false }: Props) => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const { isPending, mutate } = useDeletePool({
    onSuccessConfirm: onClose,
  });

  return (
    <>
      <ActionButton
        actionName={translate("pools.delete.title")}
        colorPalette="danger"
        icon={<FiTrash2 />}
        onClick={onOpen}
        text={translate("pools.delete.warning")}
        variant="solid"
        withText={withText}
      />

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => mutate({ poolName })}
        open={open}
        resourceName={poolName}
        title={translate("pools.delete.title")}
        warningText={translate("pools.delete.warning")}
      />
    </>
  );
};

export default DeletePoolButton;
