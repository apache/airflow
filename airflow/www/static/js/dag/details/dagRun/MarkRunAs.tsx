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

import React, { useState, useReducer } from "react";
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
import ConfirmationModal from "./ConfirmationModal";

const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props extends MenuButtonProps {
  runId: string;
  state?: RunState;
}

interface State {
  showConfirmationModal: boolean;
  confirmingAction: "success" | "failed" | null;
}

type Action =
  | { type: "SHOW_CONFIRMATION_MODAL"; payload: "success" | "failed" }
  | { type: "HIDE_CONFIRMATION_MODAL" };

const initialState = {
  showConfirmationModal: false,
  confirmingAction: null,
};

const reducer = (state: State, action: Action): State => {
  switch (action.type) {
    case "SHOW_CONFIRMATION_MODAL":
      return {
        ...state,
        showConfirmationModal: true,
        confirmingAction: action.payload,
      };
    case "HIDE_CONFIRMATION_MODAL":
      return { ...state, showConfirmationModal: false, confirmingAction: null };
    default:
      return state;
  }
};

const MarkRunAs = ({ runId, state, ...otherProps }: Props) => {
  const { mutateAsync: markFailed, isLoading: isMarkFailedLoading } =
    useMarkFailedRun(dagId, runId);
  const { mutateAsync: markSuccess, isLoading: isMarkSuccessLoading } =
    useMarkSuccessRun(dagId, runId);

  const [stateReducer, dispatch] = useReducer(reducer, initialState);

  const storedValue = localStorage.getItem("doNotShowMarkRunModal");
  const [doNotShowAgain, setDoNotShowAgain] = useState(
    storedValue ? JSON.parse(storedValue) : false
  );

  const markAsFailed = () => {
    markFailed({ confirmed: true });
  };

  const markAsSuccess = () => {
    markSuccess({ confirmed: true });
  };

  const confirmAction = () => {
    localStorage.setItem(
      "doNotShowMarkRunModal",
      JSON.stringify(doNotShowAgain)
    );
    if (stateReducer.confirmingAction === "failed") {
      markAsFailed();
    } else if (stateReducer.confirmingAction === "success") {
      markAsSuccess();
    }
    dispatch({ type: "HIDE_CONFIRMATION_MODAL" });
  };

  useKeysPress(keyboardShortcutIdentifier.dagMarkSuccess, () => {
    if (state !== "success") {
      if (!doNotShowAgain) {
        dispatch({ type: "SHOW_CONFIRMATION_MODAL", payload: "success" });
      } else markAsSuccess();
    }
  });
  useKeysPress(keyboardShortcutIdentifier.dagMarkFailed, () => {
    if (state !== "failed") {
      if (!doNotShowAgain) {
        dispatch({ type: "SHOW_CONFIRMATION_MODAL", payload: "failed" });
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
      <ConfirmationModal
        isOpen={stateReducer.showConfirmationModal}
        onClose={() => dispatch({ type: "HIDE_CONFIRMATION_MODAL" })}
        header="Confirmation"
        submitButton={
          <Button
            onClick={confirmAction}
            colorScheme={
              (stateReducer.confirmingAction === "success" && "green") ||
              (stateReducer.confirmingAction === "failed" && "red") ||
              "grey"
            }
          >
            Mark as {stateReducer.confirmingAction}
          </Button>
        }
        doNotShowAgain={doNotShowAgain}
        onDoNotShowAgainChange={(value) => setDoNotShowAgain(value)}
      >
        Are you sure you want to mark the DAG run as{" "}
        {stateReducer.confirmingAction}?
      </ConfirmationModal>
    </>
  );
};

export default MarkRunAs;
