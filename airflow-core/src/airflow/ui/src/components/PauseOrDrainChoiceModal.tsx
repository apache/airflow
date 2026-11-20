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
import type { ReactNode } from "react";

import { Button } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Modal } from "src/system-components";

type Props = {
  readonly children?: ReactNode;
  readonly displayName: string;
  readonly onChooseDrain: () => void;
  readonly onChoosePause: () => void;
  readonly onOpenChange: () => void;
  readonly open: boolean;
};

/** The "drain or pause now?" choice, shared by the single-Dag toggle and the bulk pause/drain action. */
export const PauseOrDrainChoiceModal = ({
  children,
  displayName,
  onChooseDrain,
  onChoosePause,
  onOpenChange,
  open,
}: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);

  return (
    <Modal
      footerActions={
        <>
          <Button data-testid="drain-dag" onClick={onChooseDrain}>
            {translate("dags:schedulingActions.drain")}
          </Button>
          <Button data-testid="pause-dag-now" onClick={onChoosePause} variant="outline">
            {translate("dags:schedulingActions.pauseNow")}
          </Button>
        </>
      }
      onOpenChange={onOpenChange}
      open={open}
      title={`${translate("common:pause")} ${displayName}?`}
    >
      {translate("dags:schedulingActions.drainPrompt")}
      {children}
    </Modal>
  );
};
