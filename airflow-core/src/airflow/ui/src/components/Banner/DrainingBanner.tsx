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
import { Button, HStack, Spacer, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdHourglassTop } from "react-icons/md";

import { useDagServiceGetDag } from "openapi/queries";

import { useTogglePause } from "src/queries/useTogglePause";

type Props = {
  readonly dagId: string;
};

const DrainingBanner = ({ dagId }: Props) => {
  const { t: translate } = useTranslation("dags");
  const { data: dag } = useDagServiceGetDag({ dagId });

  const { isPending, mutate } = useTogglePause({ dagId });

  if (dag?.scheduling_state !== "draining") {
    return undefined;
  }

  return (
    <HStack bg="bg.warning" color="fg.warning" px={3} py={1}>
      <MdHourglassTop />
      <Text>{translate("schedulingBanner.message")}</Text>
      <Spacer />
      <Button
        data-testid="banner-cancel-drain"
        loading={isPending}
        onClick={() => mutate({ dagId, requestBody: { scheduling_state: "active" } })}
        size="xs"
        variant="outline"
      >
        {translate("schedulingActions.cancelDrain")}
      </Button>
      <Button
        data-testid="banner-pause-now"
        loading={isPending}
        onClick={() => mutate({ dagId, requestBody: { scheduling_state: "paused" } })}
        size="xs"
        variant="outline"
      >
        {translate("schedulingActions.pauseNow")}
      </Button>
    </HStack>
  );
};

export default DrainingBanner;
