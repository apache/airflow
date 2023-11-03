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

import React, { useState } from "react";
import {
  Flex,
  Button,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  MenuButtonProps,
} from "@chakra-ui/react";
import { MdArrowDropDown } from "react-icons/md";
import { getMetaValue } from "src/utils";
import { useKeysPress } from "src/utils/useKeysPress";
import keyboardShortcutIdentifier from "src/dag/keyboardShortcutIdentifier";
import { useMarkFailedRun, useMarkSuccessRun } from "src/api";
import type { RunState } from "src/types";

import { SimpleStatus } from "../../StatusBox";
import ActionModal from "./ActionModal";

const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props extends MenuButtonProps {
  runId: string;
  state?: RunState;
}

const MarkRunAs = ({ runId, state, ...otherProps }: Props) => {
  const { mutateAsync: markFailed, isLoading: isMarkFailedLoading } =
    useMarkFailedRun(dagId, runId);
  const { mutateAsync: markSuccess, isLoading: isMarkSuccessLoading } =
    useMarkSuccessRun(dagId, runId);

  const [showConfirmationModal, setShowConfirmationModal] = useState(false);
  const [confirmingAction, setConfirmingAction] = useState<
    "success" | "failed"
  >();
  const [doNotShowAgainSuccess, setDoNotShowAgainSuccess] = useState(false);
  const [doNotShowAgainFailed, setDoNotShowAgainFailed] = useState(false);
  const markAsFailed = () => {
    markFailed({ confirmed: true });
  };

  const markAsSuccess = () => {
    markSuccess({ confirmed: true });
  };

  const confirmAction = () => {
    if (confirmingAction === "failed") {
      markAsFailed();
    } else if (confirmingAction === "success") {
      markAsSuccess();
    }
    setShowConfirmationModal(false);
  };

  useKeysPress(keyboardShortcutIdentifier.dagMarkSuccess, () => {
    if (state !== "success") {
      if (!doNotShowAgainSuccess) {
        setConfirmingAction("success");
        setShowConfirmationModal(true);
      } else markAsSuccess();
    }
  });
  useKeysPress(keyboardShortcutIdentifier.dagMarkFailed, () => {
    if (state !== "failed") {
      if (!doNotShowAgainFailed) {
        setConfirmingAction("failed");
        setShowConfirmationModal(true);
      } else markAsFailed();
    }
  });

  const markLabel = "Manually set dag run state";
  return (
    <>
      <Menu>
        <MenuButton
          as={Button}
          colorScheme="blue"
          transition="all 0.2s"
          title={markLabel}
          aria-label={markLabel}
          disabled={!canEdit || isMarkFailedLoading || isMarkSuccessLoading}
          {...otherProps}
          mt={2}
        >
          <Flex>
            Mark state as...
            <MdArrowDropDown size="16px" />
          </Flex>
        </MenuButton>
        <MenuList>
          <MenuItem onClick={markAsFailed} isDisabled={state === "failed"}>
            <SimpleStatus state="failed" mr={2} />
            failed
          </MenuItem>
          <MenuItem onClick={markAsSuccess} isDisabled={state === "success"}>
            <SimpleStatus state="success" mr={2} />
            success
          </MenuItem>
        </MenuList>
      </Menu>
      <ActionModal
        isOpen={showConfirmationModal}
        onClose={() => setShowConfirmationModal(false)}
        header="Confirmation"
        submitButton={
          <Button
            onClick={confirmAction}
            colorScheme={
              (confirmingAction === "success" && "green") ||
              (confirmingAction === "failed" && "red") ||
              "grey"
            }
          >
            Mark as {confirmingAction}
          </Button>
        }
        doNotShowAgain={
          confirmingAction === "success"
            ? doNotShowAgainSuccess
            : doNotShowAgainFailed
        }
        onDoNotShowAgainChange={(value) => {
          if (confirmingAction === "success") {
            setDoNotShowAgainSuccess(value);
          } else if (confirmingAction === "failed") {
            setDoNotShowAgainFailed(value);
          }
        }}
      >
        This marks the DAG run as {confirmingAction}. Are you sure you want to
        proceed?
      </ActionModal>
    </>
  );
};

export default MarkRunAs;
