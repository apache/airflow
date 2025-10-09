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
import { type ButtonProps, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiTrash2 } from "react-icons/fi";
import { useLocation, useNavigate } from "react-router-dom";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import DeleteDialog from "src/components/DeleteDialog";
import ActionButton from "src/components/ui/ActionButton";
import { useDeleteDagRun } from "src/queries/useDeleteDagRun";

type DeleteRunButtonProps = {
  readonly dagRun: DAGRunResponse;
  readonly variant?: string;
  readonly withText?: boolean;
} & Omit<ButtonProps, "colorPalette" | "onClick" | "variant">;

const DeleteRunButton = ({ dagRun, variant, withText = true, ...rest }: DeleteRunButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const navigate = useNavigate();
  const location = useLocation();
  const { t: translate } = useTranslation();

  const isOnRunDetailPage = location.pathname.includes(`/dags/${dagRun.dag_id}/runs/${dagRun.dag_run_id}`);

  const { isPending, mutate: deleteDagRun } = useDeleteDagRun({
    dagId: dagRun.dag_id,
    dagRunId: dagRun.dag_run_id,
    onSuccessConfirm: () => {
      onClose();
      if (isOnRunDetailPage) {
        navigate(`/dags/${dagRun.dag_id}/runs`);
      }
    },
  });

  return (
    <>
      <ActionButton
        actionName={translate("dags:runAndTaskActions.delete.button", { type: translate("dagRun_one") })}
        colorPalette="danger"
        icon={<FiTrash2 />}
        onClick={onOpen}
        text={translate("dags:runAndTaskActions.delete.button", { type: translate("dagRun_one") })}
        variant={variant}
        withText={withText}
        {...rest}
      />

      <DeleteDialog
        isDeleting={isPending}
        onClose={onClose}
        onDelete={() => {
          deleteDagRun({
            dagId: dagRun.dag_id,
            dagRunId: dagRun.dag_run_id,
          });
        }}
        open={open}
        resourceName={translate("dags:runAndTaskActions.delete.dialog.resourceName", {
          id: dagRun.dag_run_id,
          type: translate("dagRun_one"),
        })}
        title={translate("dags:runAndTaskActions.delete.dialog.title", { type: translate("dagRun_one") })}
        warningText={translate("dags:runAndTaskActions.delete.dialog.warning", {
          type: translate("dagRun_one"),
        })}
      />
    </>
  );
};

export default DeleteRunButton;
