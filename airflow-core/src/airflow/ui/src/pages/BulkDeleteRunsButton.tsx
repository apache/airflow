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

import DeleteDialog from "src/components/DeleteDialog";
import ActionButton from "src/components/ui/ActionButton";
import { useBulkDeleteDagRuns, type SelectedRun } from "src/queries/useBulkDeleteDagRuns";

type BulkDeleteRunsButtonProps = {
  readonly onSuccessConfirm: () => void;
  readonly selectedRuns: Array<SelectedRun>;
  readonly variant?: string;
  readonly withText?: boolean;
} & Omit<ButtonProps, "colorPalette" | "disabled" | "onClick" | "variant">;

const BulkDeleteRunsButton = ({
  onSuccessConfirm,
  selectedRuns,
  variant,
  withText = false,
  ...rest
}: BulkDeleteRunsButtonProps) => {
  const { onClose, onOpen, open } = useDisclosure();
  const { t: translate } = useTranslation();

  const { bulkDelete, isDeleting } = useBulkDeleteDagRuns(() => {
    onClose();
    onSuccessConfirm();
  });

  const count = selectedRuns.length;

  return (
    <>
      <ActionButton
        actionName={translate("dags:runAndTaskActions.delete.button", { type: translate("dagRun_other") })}
        colorPalette="danger"
        disabled={count === 0}
        icon={<FiTrash2 />}
        onClick={onOpen}
        text={translate("dags:runAndTaskActions.delete.button", { type: translate("dagRun_other") })}
        variant={variant}
        withText={withText}
        {...rest}
      />

      <DeleteDialog
        isDeleting={isDeleting}
        onClose={onClose}
        onDelete={() => void bulkDelete(selectedRuns)}
        open={open}
        resourceName={`${count} ${translate("dagRun", { count })}`}
        title={translate("dags:runAndTaskActions.delete.dialog.title", { type: translate("dagRun_other") })}
        warningText={translate("dags:runAndTaskActions.delete.dialog.warning", {
          type: translate("dagRun_other"),
        })}
      />
    </>
  );
};

export default BulkDeleteRunsButton;
