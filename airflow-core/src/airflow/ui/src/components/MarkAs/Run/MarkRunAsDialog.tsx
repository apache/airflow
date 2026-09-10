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
import { useEffect, useState } from "react";

import { Button } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { DagRunMutableStates, DAGRunResponse } from "openapi/requests/types.gen";

import { Modal } from "src/system-components";

import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";

import { usePatchDagRun } from "src/queries/usePatchDagRun";

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly state: DagRunMutableStates;
};

const MarkRunAsDialog = ({ dagRun, onClose, open, state }: Props) => {
  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;
  const { t: translate } = useTranslation();

  const [note, setNote] = useState<string | null>(dagRun.note);

  useEffect(() => {
    if (open) {
      setNote(dagRun.note);
    }
  }, [dagRun.note, open]);

  const handleClose = () => {
    setNote(dagRun.note);
    onClose();
  };
  const { isPending, mutate } = usePatchDagRun({ dagId, dagRunId, onSuccess: handleClose });

  return (
    <Modal
      footerActions={
        <Button
          data-testid="mark-run-as-confirm"
          loading={isPending}
          onClick={() => {
            mutate({
              dagId,
              dagRunId,
              requestBody: { note, state },
            });
          }}
        >
          {translate("modal.confirm")}
        </Button>
      }
      lazyMount
      onOpenChange={(details) => {
        if (!details.open) {
          handleClose();
        }
      }}
      open={open}
      title={
        <>
          {translate("dags:runAndTaskActions.markAs.title", {
            state,
            type: translate("dagRun_one"),
          })}
          : {dagRunId} <StateBadge state={state} />
        </>
      }
    >
      <ActionAccordion note={note} setNote={setNote} />
    </Modal>
  );
};

export default MarkRunAsDialog;
