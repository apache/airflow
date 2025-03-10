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
import { useState } from "react";
import { MdClose, MdPause, MdPlayArrow, MdStop } from "react-icons/md";

import {
  useBackfillServiceCancelBackfill,
  useBackfillServiceListBackfills,
  useBackfillServicePauseBackfill,
  useBackfillServiceUnpauseBackfill,
} from "openapi/queries";

import Time from "../Time";
import { ProgressBar } from "../ui";
import ActionButton from "../ui/ActionButton";

type Props = {
  readonly dagId: string;
};

const BackfillBanner = ({ dagId }: Props) => {
  const { data, isLoading } = useBackfillServiceListBackfills({
    dagId,
  });

  const backfill = data?.backfills[0];

  const { isPending: isPausePending, mutate: pauseMutate } = useBackfillServicePauseBackfill();
  const { isPending: isUnPausePending, mutate: unpauseMutate } = useBackfillServiceUnpauseBackfill();

  const { isPending: isStopPending, mutate: stopPending } = useBackfillServiceCancelBackfill();

  let initialVisibility = false;

  if (data?.total_entries !== undefined && data.total_entries > 0) {
    initialVisibility = true;
  }
  const [isVisible, setIsVisible] = useState<boolean>(initialVisibility);
  const [isActive, setIsActive] = useState(false);
  const [isDisabled, setIsDisabled] = useState<boolean>(false);
  const togglePause = () => {
    if (isActive) {
      pauseMutate({ backfillId: backfill?.id });
    } else {
      unpauseMutate({ backfillId: backfill?.id });
    }
    setIsActive(!isActive);
  };

  const cancel = () => {
    stopPending({ backfillId: backfill?.id });
    setIsDisabled(!isDisabled);
  };

  return isVisible && !isLoading ? (
    <Box bg="blue.solid" color="white" fontSize="m" mr="1.5" my="1" px="2" py="1" rounded="lg">
      <HStack>
        <Text key="backfill">Backfill in progress:</Text>
        <>
          <Time datetime={data?.backfills[0]?.from_date} /> - <Time datetime={data?.backfills[0]?.to_date} />
        </>
        <Spacer flex="max-content" />
        <ProgressBar key="progressbar" size="xs" visibility="visible" />
        <ActionButton
          actionName=""
          disabled={isDisabled}
          icon={isActive ? <MdPlayArrow color="white" size="1" /> : <MdPause color="white" size="1" />}
          loading={isPausePending || isUnPausePending}
          onClick={() => {
            togglePause();
          }}
          rounded="full"
          size="xs"
          text=""
          variant="outline"
        />
        <ActionButton
          actionName=""
          disabled={isDisabled}
          icon={<MdStop color="white" size="1" />}
          loading={isStopPending}
          onClick={() => {
            cancel();
          }}
          rounded="full"
          size="xs"
          text=""
          variant="outline"
        />
        <ActionButton
          actionName=""
          icon={<MdClose color="white" size="1" />}
          onClick={() => setIsVisible(false)}
          rounded="full"
          size="xs"
          text=""
          variant="outline"
        />
      </HStack>
    </Box>
  ) : (
    ""
  );
};

export default BackfillBanner;
