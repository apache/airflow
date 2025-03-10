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
import { MdClose, MdPause, MdStop } from "react-icons/md";

import { useBackfillServiceListBackfills } from "openapi/queries";

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

  let initialVisibility = false;

  if (data?.total_entries !== undefined && data.total_entries > 0) {
    initialVisibility = true;
  }
  const [visible, setVisible] = useState<boolean>(initialVisibility);

  return visible && !isLoading ? (
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
          icon={<MdPause color="white" size="xs" />}
          key="pause"
          rounded="full"
          size="xs"
          text=""
          variant="outline"
        />
        <ActionButton
          actionName=""
          icon={<MdStop color="white" size="xs" />}
          key="stop"
          rounded="full"
          size="xs"
          text=""
          variant="outline"
        />
        <ActionButton
          actionName=""
          icon={<MdClose color="white" size="xs" />}
          key="close"
          onClick={() => setVisible(false)}
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
