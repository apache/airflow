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
import { FiZap } from "react-icons/fi";

import { useDagServiceGetDagDetails, useTriggerServiceGetTriggers } from "openapi/queries";

import { StatusChip } from "src/components/StatusChip";

import { FailingWatchersModal, getFailingWatchersParams } from "./FailingWatchersModal";

/** Failing watchers on the assets the Dag is scheduled on, which keep it from running. */
export const DagFailingWatchersChip = ({ dagId }: { readonly dagId: string }) => {
  const { t: translate } = useTranslation("dag");
  const { onClose, onOpen, open } = useDisclosure();
  // The details the breadcrumb already loads: only a Dag scheduled on assets asks for its watchers.
  const { data: dag } = useDagServiceGetDagDetails({ dagId }, undefined, { enabled: dagId !== "" });
  // Only the count is needed: the modal reads them a page at a time.
  const { data } = useTriggerServiceGetTriggers({ ...getFailingWatchersParams(dagId), limit: 0 }, undefined, {
    enabled: Boolean(dag?.asset_expression),
  });
  const count = data?.total_entries ?? 0;

  if (count === 0) {
    return undefined;
  }

  return (
    <>
      <StatusChip
        icon={<FiZap />}
        label={translate("failingWatcherTriggers", { count })}
        onClick={onOpen}
        severity="error"
      />
      <FailingWatchersModal dagId={dagId} onClose={onClose} open={open} />
    </>
  );
};
