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
import { useClearRun, useQueueRun } from "src/api";
import ConfirmationModal from "./ConfirmationModal";

const canEdit = getMetaValue("can_edit") === "True";
const dagId = getMetaValue("dag_id");

interface Props extends MenuButtonProps {
  runId: string;
}

interface State {
  showConfirmationModal: boolean;
  confirmingAction: "existing" | "failed" | "queue" | null;
}

type Action =
  | {
      type: "SHOW_CONFIRMATION_MODAL";
      payload: "existing" | "failed" | "queue";
    }
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

const ClearRun = ({ runId, ...otherProps }: Props) => {
  const { mutateAsync: onClear, isLoading: isClearLoading } = useClearRun(
    dagId,
    runId
  );

  const { mutateAsync: onQueue, isLoading: isQueueLoading } = useQueueRun(
    dagId,
    runId
  );

  const clearExistingTasks = () => {
    onClear({ confirmed: true });
  };

  const clearFailedTasks = () => {
    onClear({ confirmed: true, only_failed: true });
  };

  const queueNewTasks = () => {
    onQueue({ confirmed: true });
  };

  const [stateReducer, dispatch] = useReducer(reducer, initialState);

  const storedValue = localStorage.getItem("doNotShowClearRunModal");
  const [doNotShowAgain, setDoNotShowAgain] = useState(
    storedValue ? JSON.parse(storedValue) : false
  );

  const confirmClearExisting = () => {
    if (!doNotShowAgain) {
      dispatch({ type: "SHOW_CONFIRMATION_MODAL", payload: "existing" });
    } else clearExistingTasks();
  };

  const confirmClearFailed = () => {
    if (!doNotShowAgain) {
      dispatch({ type: "SHOW_CONFIRMATION_MODAL", payload: "failed" });
    } else clearFailedTasks();
  };

  const confirmQueued = () => {
    if (!doNotShowAgain) {
      dispatch({ type: "SHOW_CONFIRMATION_MODAL", payload: "queue" });
    } else queueNewTasks();
  };

  const confirmAction = () => {
    localStorage.setItem(
      "doNotShowClearRunModal",
      JSON.stringify(doNotShowAgain)
    );
    if (stateReducer.confirmingAction === "failed") {
      clearFailedTasks();
    } else if (stateReducer.confirmingAction === "existing") {
      clearExistingTasks();
    } else if (stateReducer.confirmingAction === "queue") {
      queueNewTasks();
    }
    dispatch({ type: "HIDE_CONFIRMATION_MODAL" });
  };

  useKeysPress(keyboardShortcutIdentifier.dagRunClear, confirmClearExisting);

  const clearLabel = "Clear tasks or add new tasks";
  return (
    <>
      <Menu>
        <MenuButton
          as={Button}
          colorScheme="blue"
          transition="all 0.2s"
          title={clearLabel}
          aria-label={clearLabel}
          disabled={!canEdit || isClearLoading || isQueueLoading}
          {...otherProps}
          mt={2}
        >
          <Flex>
            Clear
            <MdArrowDropDown size="16px" />
          </Flex>
        </MenuButton>
        <MenuList>
          <MenuItem onClick={confirmClearExisting}>
            Clear existing tasks
          </MenuItem>
          <MenuItem onClick={confirmClearFailed}>
            Clear only failed tasks
          </MenuItem>
          <MenuItem onClick={confirmQueued}>Queue up new tasks</MenuItem>
        </MenuList>
      </Menu>
      <ConfirmationModal
        isOpen={stateReducer.showConfirmationModal}
        onClose={() => dispatch({ type: "HIDE_CONFIRMATION_MODAL" })}
        header="Confirmation"
        submitButton={
          <Button onClick={confirmAction} colorScheme="blue">
            Clear DAG run
          </Button>
        }
        doNotShowAgain={doNotShowAgain}
        onDoNotShowAgainChange={(value) => setDoNotShowAgain(value)}
      >
        This DAG run will be cleared. Are you sure you want to proceed?
      </ConfirmationModal>
    </>
  );
};

export default ClearRun;
