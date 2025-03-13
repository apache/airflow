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
import { Box, HStack, Spacer, Text } from "@chakra-ui/react";
import { MdPause, MdPlayArrow, MdStop } from "react-icons/md";

import {
  useBackfillServiceCancelBackfill,
  useBackfillServiceListBackfills,
  useBackfillServiceListBackfillsKey,
  useBackfillServicePauseBackfill,
  useBackfillServiceUnpauseBackfill,
} from "openapi/queries";
import { queryClient } from "src/queryClient";

import Time from "../Time";
import { Button, ProgressBar } from "../ui";

type Props = {
  readonly dagId: string;
};

const onSuccess = async () => {
  await queryClient.invalidateQueries({
    queryKey: [useBackfillServiceListBackfillsKey],
  });
};

const BackfillBanner = ({ dagId }: Props) => {
  const { data, isLoading } = useBackfillServiceListBackfills({
    dagId,
  });
  const [backfill] = data?.backfills.filter((bf) => bf.completed_at === null) ?? [];

  const { isPending: isPausePending, mutate: pauseMutate } = useBackfillServicePauseBackfill({ onSuccess });
  const { isPending: isUnPausePending, mutate: unpauseMutate } = useBackfillServiceUnpauseBackfill({
    onSuccess,
  });

  const { isPending: isStopPending, mutate: stopPending } = useBackfillServiceCancelBackfill({ onSuccess });

  const togglePause = () => {
    if (backfill?.is_paused) {
      unpauseMutate({ backfillId: backfill.id });
    } else {
      pauseMutate({ backfillId: backfill?.id });
    }
  };

  const cancel = () => {
    stopPending({ backfillId: backfill?.id });
  };

  if (isLoading || backfill === undefined) {
    return undefined;
  }

  return (
    <Box bg="blue.solid" color="white" fontSize="m" mr="0.5" my="1" px="2" py="1" rounded="lg">
      <HStack>
        <Text key="backfill">Backfill in progress:</Text>
        <>
          <Time datetime={data?.backfills[0]?.from_date} /> - <Time datetime={data?.backfills[0]?.to_date} />
        </>
        <Spacer flex="max-content" />
        <ProgressBar size="xs" visibility="visible" />
        <Button
          aria-label={backfill.is_paused ? "Unpause backfill" : "Pause backfill"}
          loading={isPausePending || isUnPausePending}
          onClick={() => {
            togglePause();
          }}
          rounded="full"
          size="xs"
          variant="outline"
        >
          {backfill.is_paused ? <MdPlayArrow color="white" size="1" /> : <MdPause color="white" size="1" />}
        </Button>
        <Button
          aria-label="Cancel backfill"
          loading={isStopPending}
          onClick={() => {
            cancel();
          }}
          rounded="full"
          size="xs"
          variant="outline"
        >
          <MdStop color="white" size="1" />
        </Button>
      </HStack>
    </Box>
  );
};

export default BackfillBanner;
