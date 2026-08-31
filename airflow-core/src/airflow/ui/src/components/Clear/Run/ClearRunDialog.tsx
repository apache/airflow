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
import { Button, Flex } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { CgRedo } from "react-icons/cg";

import { useDagServiceGetDagDetails } from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { getRunOnLatestVersionState } from "src/components/Clear/TaskInstance/runOnLatestVersion";
import { useRerunWithLatestVersion } from "src/components/Clear/useRerunWithLatestVersion";
import { useClearRunDefaultOptions } from "src/hooks/useUserSettings";
import { useClearDagRunDryRun } from "src/queries/useClearDagRunDryRun";
import { useClearDagRun } from "src/queries/useClearRun";
import { Checkbox, Modal, SegmentedControl } from "src/system-components";
import { isStatePending, useAutoRefresh } from "src/utils";

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly onClose: () => void;
  readonly open: boolean;
};

const ClearRunDialog = ({ dagRun, onClose, open }: Props) => {
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
  const [clearRunDefaultOptions] = useClearRunDefaultOptions();
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(clearRunDefaultOptions);
  const onlyFailed = selectedOptions.includes("onlyFailed");
  const onlyNew = selectedOptions.includes("newTasks");

  const { data: dagDetails } = useDagServiceGetDagDetails({
    dagId,
  });

  // Offered only where it changes the outcome. A non-versioned bundle (e.g. LocalDagBundle)
  // leaves bundle_version null and resolves to the latest serialized Dag at run time anyway,
  // so unless the run has no version at all the option would be a no-op there.
  const { runOnLatestVersionForced, shouldShowRunOnLatestOption } = getRunOnLatestVersionState({
    latestBundleVersion: dagDetails?.bundle_version,
    latestDagVersionNumber: dagDetails?.latest_dag_version?.version_number,
    selectedBundleVersion: dagRun.bundle_version,
    selectedDagVersionNumber: dagRun.dag_versions.at(-1)?.version_number,
    selectedVersionMissing: dagRun.dag_versions.length === 0,
  });

  const { setValue: setRunOnLatestVersion, value: runOnLatestVersion } = useRerunWithLatestVersion({
    dagLevelConfig: dagDetails?.rerun_with_latest_version,
  });

  const refetchInterval = useAutoRefresh({ dagId });

  const { data: affectedTasks = { task_instances: [], total_entries: 0 } } = useClearDagRunDryRun({
    dagId,
    dagRunId,
    options: {
      enabled: open,
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => "state" in ti && isStatePending(ti.state))
          ? refetchInterval
          : false,
    },
    requestBody: {
      only_failed: onlyFailed,
      only_new: onlyNew,
      run_on_latest_version: runOnLatestVersion,
    },
  });

  const { isPending, mutate } = useClearDagRun({
    dagId,
    dagRunId,
    onSuccessConfirm: handleClose,
  });

  const shouldShowBundleVersionOption = shouldShowRunOnLatestOption && !onlyNew;

  return (
    <Modal
      footerActions={
        <>
          <Button
            disabled={affectedTasks.total_entries === 0}
            loading={isPending}
            onClick={() => {
              mutate({
                dagId,
                dagRunId,
                requestBody: {
                  dry_run: false,
                  note: note === dagRun.note ? undefined : note,
                  only_failed: onlyFailed,
                  only_new: onlyNew,
                  run_on_latest_version: runOnLatestVersion,
                },
              });
            }}
          >
            <CgRedo /> {translate("modal.confirm")}
          </Button>
          {shouldShowBundleVersionOption ? (
            <Checkbox
              checked={runOnLatestVersionForced || runOnLatestVersion}
              disabled={runOnLatestVersionForced}
              onCheckedChange={(event) => setRunOnLatestVersion(Boolean(event.checked))}
              title={
                runOnLatestVersionForced
                  ? translate("dags:runAndTaskActions.options.runOnLatestVersionForced")
                  : undefined
              }
            >
              {translate("dags:runAndTaskActions.options.runOnLatestVersion")}
            </Checkbox>
          ) : undefined}
        </>
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
          <strong>
            {translate("dags:runAndTaskActions.clear.title", { type: translate("dagRun_one") })}:{" "}
          </strong>{" "}
          {dagRunId}
        </>
      }
    >
      <Flex justifyContent="center">
        <SegmentedControl
          defaultValues={clearRunDefaultOptions}
          onChange={setSelectedOptions}
          options={[
            {
              label: translate("dags:runAndTaskActions.options.existingTasks"),
              value: "existingTasks",
            },
            {
              label: translate("dags:runAndTaskActions.options.onlyFailed"),
              value: "onlyFailed",
            },
            {
              label: translate("dags:runAndTaskActions.options.queueNew"),
              value: "newTasks",
            },
          ]}
        />
      </Flex>
      <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
    </Modal>
  );
};

export default ClearRunDialog;
