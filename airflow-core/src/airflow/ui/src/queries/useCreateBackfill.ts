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
import { useState } from "react";

import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import {
  UseDagServiceGetDagDetailsKeyFn,
  UseDagServiceGetDagKeyFn,
  useBackfillServiceCreateBackfill,
  useBackfillServiceListBackfillsUiKey,
  useDagServiceGetDagsUiKey,
} from "openapi/queries";
import type { BackfillResponse, CreateBackfillData } from "openapi/requests/types.gen";

import { toaster } from "src/system-components";

export const useCreateBackfill = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const [dateValidationError, setDateValidationError] = useState<unknown>(undefined);
  const [error, setError] = useState<unknown>(undefined);
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("components");

  const onSuccess = async (backfill: BackfillResponse, variables: CreateBackfillData) => {
    const { dag_id: dagId } = backfill;
    const drainKeys = variables.requestBody.drain_dag
      ? [
          UseDagServiceGetDagKeyFn({ dagId }, [{ dagId }]),
          UseDagServiceGetDagDetailsKeyFn({ dagId }, [{ dagId }]),
          [useDagServiceGetDagsUiKey],
        ]
      : [];

    await Promise.all(
      [[useBackfillServiceListBackfillsUiKey], ...drainKeys].map((queryKey) =>
        queryClient.invalidateQueries({ queryKey }),
      ),
    );
    toaster.create({
      description: translate("backfill.toaster.success.description"),
      title: translate("backfill.toaster.success.title"),
      type: "success",
    });
    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useBackfillServiceCreateBackfill({ onError, onSuccess });

  const createBackfill = (data: CreateBackfillData) => {
    if (data.requestBody.from_date === "" || data.requestBody.to_date === "") {
      setDateValidationError({
        body: {
          detail: translate("backfill.validation.datesRequired"),
        },
      });

      return;
    }

    const dataIntervalStart = new Date(data.requestBody.from_date);
    const dataIntervalEnd = new Date(data.requestBody.to_date);
    const dagId = data.requestBody.dag_id;

    if (dataIntervalStart > dataIntervalEnd) {
      setDateValidationError({
        body: {
          detail: translate("backfill.validation.startBeforeEnd"),
        },
      });

      return;
    }

    const formattedDataIntervalStart = dataIntervalStart.toISOString();
    const formattedDataIntervalEnd = dataIntervalEnd.toISOString();

    mutate({
      requestBody: {
        dag_id: dagId,
        dag_run_conf: data.requestBody.dag_run_conf,
        drain_dag: data.requestBody.drain_dag,
        from_date: formattedDataIntervalStart,
        max_active_runs: data.requestBody.max_active_runs,
        reprocess_behavior: data.requestBody.reprocess_behavior,
        run_backwards: data.requestBody.run_backwards,
        run_on_latest_version: data.requestBody.run_on_latest_version,
        to_date: formattedDataIntervalEnd,
      },
    });
  };

  return { createBackfill, dateValidationError, error, isPending };
};
