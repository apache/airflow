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
import { Box, type ButtonProps, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";
import { useLocation, useNavigate } from "react-router-dom";

import DeleteDialog from "src/components/DeleteDialog";
import ActionButton from "src/components/ui/ActionButton";
import { useDeleteDag } from "src/queries/useDeleteDag";

type DeleteDagButtonProps = {
  readonly dagDisplayName: string;
  readonly dagId: string;
  readonly withText?: boolean;
} & ButtonProps;

const DeleteDagButton = ({ dagDisplayName, dagId, width, withText = true }: DeleteDagButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const navigate = useNavigate();
  const location = useLocation();
  const { t: translate } = useTranslation("dags");

  const isOnDagDetailPage = location.pathname.includes(`/dags/${dagId}`);

  const { isPending, mutate: deleteDag } = useDeleteDag({
    dagId,
    onSuccessConfirm: () => {
      onClose();
      if (isOnDagDetailPage) {
        navigate("/dags");
      }
    },
  });

  return (
    <Box width={width}>
      <ActionButton
        actionName={translate("dagActions.delete.button")}
        colorPalette="danger"
        icon={<FiTrash2 />}
        onClick={onOpen}
        text={translate("dagActions.delete.button")}
        width={width}
        withText={withText}
      />

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => deleteDag({ dagId })}
        open={open}
        resourceName={dagDisplayName}
        title={translate("dagActions.delete.button")}
        warningText={translate("dagActions.delete.warning")}
      />
    </Box>
  );
};

export default DeleteDagButton;
