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
import { Box, HStack, Spacer, Text, type ButtonProps } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { MdPause, MdPlayArrow, MdStop } from "react-icons/md";
import { RiArrowGoBackFill } from "react-icons/ri";

import {
  useBackfillServiceCancelBackfill,
  useBackfillServiceListBackfillsUi,
  useBackfillServiceListBackfillsUiKey,
  useBackfillServicePauseBackfill,
  useBackfillServiceUnpauseBackfill,
} from "openapi/queries";
import type { BackfillResponse } from "openapi/requests/types.gen";
import { useAutoRefresh } from "src/utils";

import Time from "../Time";
import { Button, ProgressBar } from "../ui";

type Props = {
  readonly dagId: string;
};

const buttonProps = {
  _hover: { bg: "info.contrast", color: "info.muted" },
  borderColor: "info.contrast",
  color: "info.contrast",
  rounded: "full",
  size: "xs",
  variant: "outline",
} satisfies ButtonProps;

const BackfillBanner = ({ dagId }: Props) => {
  const { t: translate } = useTranslation("components");
  const refetchInterval = useAutoRefresh({ dagId });

  const { data, isLoading } = useBackfillServiceListBackfillsUi(
    {
      dagId,
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.backfills.some((bf: BackfillResponse) => bf.completed_at === null && !bf.is_paused)
          ? refetchInterval
          : false,
    },
  );
  const [backfill] = data?.backfills.filter((bf: BackfillResponse) => bf.completed_at === null) ?? [];

  const queryClient = useQueryClient();
  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useBackfillServiceListBackfillsUiKey],
    });
  };

  const { isPending: isPausePending, mutate: pauseMutate } = useBackfillServicePauseBackfill({ onSuccess });
  const { isPending: isUnPausePending, mutate: unpauseMutate } = useBackfillServiceUnpauseBackfill({
    onSuccess,
  });
  const { isPending: isStopPending, mutate: stopPending } = useBackfillServiceCancelBackfill({ onSuccess });

  const togglePause = () => {
    if (backfill === undefined) {
      return;
    }
    if (backfill.is_paused) {
      unpauseMutate({ backfillId: backfill.id });
    } else {
      pauseMutate({ backfillId: backfill.id });
    }
  };

  const cancel = () => {
    if (backfill === undefined) {
      return;
    }
    stopPending({ backfillId: backfill.id });
  };

  if (isLoading || backfill === undefined) {
    return undefined;
  }

  return (
    <Box bg="info.solid" borderRadius="full" color="info.contrast" my="1" px="2" py="1">
      <HStack alignItems="center" ml={3}>
        <RiArrowGoBackFill />
        <Text key="backfill">{translate("banner.backfillInProgress")}:</Text>
        <Text fontSize="sm">
          {" "}
          <Time datetime={data?.backfills[0]?.from_date} /> - <Time datetime={data?.backfills[0]?.to_date} />
        </Text>

        <Spacer flex="max-content" />
        <ProgressBar size="xs" visibility="visible" />
        <Button
          aria-label={backfill.is_paused ? translate("banner.unpause") : translate("banner.pause")}
          loading={isPausePending || isUnPausePending}
          onClick={() => {
            togglePause();
          }}
          {...buttonProps}
        >
          {backfill.is_paused ? <MdPlayArrow /> : <MdPause />}
        </Button>
        <Button
          aria-label={translate("banner.cancel")}
          loading={isStopPending}
          onClick={() => {
            cancel();
          }}
          {...buttonProps}
        >
          <MdStop />
        </Button>
      </HStack>
    </Box>
  );
};

export default BackfillBanner;
