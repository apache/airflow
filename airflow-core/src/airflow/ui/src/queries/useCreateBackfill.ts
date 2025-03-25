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

import { useBackfillServiceCreateBackfill, useBackfillServiceListBackfillsKey } from "openapi/queries";
import type { CreateBackfillData } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";
import { queryClient } from "src/queryClient";

export const useCreateBackfill = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const [dateValidationError, setDateValidationError] = useState<unknown>(undefined);
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useBackfillServiceListBackfillsKey],
    });
    toaster.create({
      description: "Backfill jobs have been successfully triggered.",
      title: "Backfill generated",
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
          detail: "Both Data Interval Start Date and End Date must be provided.",
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
          detail: "Data Interval Start Date must be less than or equal to Data Interval End Date.",
        },
      });

      return;
    }

    const formattedDataIntervalStart = dataIntervalStart.toISOString();
    const formattedDataIntervalEnd = dataIntervalEnd.toISOString();

    mutate({
      requestBody: {
        dag_id: dagId,
        dag_run_conf: {},
        from_date: formattedDataIntervalStart,
        max_active_runs: data.requestBody.max_active_runs,
        reprocess_behavior: data.requestBody.reprocess_behavior,
        run_backwards: false,
        to_date: formattedDataIntervalEnd,
      },
    });
  };

  return { createBackfill, dateValidationError, error, isPending };
};
