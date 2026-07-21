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
import { Button, HStack, Text } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { MdPause, MdPlayArrow, MdStop } from "react-icons/md";
import { RiArrowGoBackFill } from "react-icons/ri";

import {
  useBackfillServiceCancelBackfill,
  useBackfillServiceGetBackfillKey,
  useBackfillServicePauseBackfill,
  useBackfillServiceUnpauseBackfill,
} from "openapi/queries";
import type { BackfillResponse } from "openapi/requests/types.gen";
import { HeaderCard } from "src/components/HeaderCard";
import Time from "src/components/Time";
import { getDuration } from "src/utils";

export const Header = ({ backfill }: { readonly backfill: BackfillResponse }) => {
  const { t: translate } = useTranslation();
  const isCompleted = backfill.completed_at !== null;
  const queryClient = useQueryClient();
  const onSuccess = async () => {
    await queryClient.invalidateQueries({ queryKey: [useBackfillServiceGetBackfillKey] });
  };
  const { isPending: isCancelPending, mutate: cancelBackfill } = useBackfillServiceCancelBackfill({
    onSuccess,
  });
  const { isPending: isPausePending, mutate: pauseBackfill } = useBackfillServicePauseBackfill({ onSuccess });
  const { isPending: isUnpausePending, mutate: unpauseBackfill } = useBackfillServiceUnpauseBackfill({
    onSuccess,
  });

  const togglePause = () => {
    if (backfill.is_paused) {
      unpauseBackfill({ backfillId: backfill.id });
    } else {
      pauseBackfill({ backfillId: backfill.id });
    }
  };

  return (
    <HeaderCard
      actions={
        isCompleted ? undefined : (
          <>
            <Button
              aria-label={
                backfill.is_paused
                  ? translate("components:banner.unpause")
                  : translate("components:banner.pause")
              }
              loading={isPausePending || isUnpausePending}
              onClick={togglePause}
              size="sm"
              variant="outline"
            >
              {backfill.is_paused ? <MdPlayArrow /> : <MdPause />}
            </Button>
            <Button
              aria-label={translate("components:banner.cancel")}
              loading={isCancelPending}
              onClick={() => {
                cancelBackfill({ backfillId: backfill.id });
              }}
              size="sm"
              variant="outline"
            >
              <MdStop />
            </Button>
          </>
        )
      }
      icon={<RiArrowGoBackFill />}
      stats={[
        {
          label: translate("table.from"),
          value: <Time datetime={backfill.from_date} />,
        },
        {
          label: translate("table.to"),
          value: <Time datetime={backfill.to_date} />,
        },
        {
          label: translate("table.createdAt"),
          value: <Time datetime={backfill.created_at} />,
        },
        {
          label: translate("duration"),
          value: isCompleted ? getDuration(backfill.created_at, backfill.completed_at) : "—",
        },
      ]}
      title={
        <HStack>
          <Text>
            {translate("common:backfill_one")} #{backfill.id}
          </Text>
        </HStack>
      }
    />
  );
};
