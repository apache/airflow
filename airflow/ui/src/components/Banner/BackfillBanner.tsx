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
import { Text } from "@chakra-ui/react";
import { useState } from "react";
import { MdClose, MdPause, MdStop } from "react-icons/md";

import { useBackfillServiceListBackfills } from "openapi/queries";
import type { DAGResponse } from "openapi/requests/types.gen";

import Time from "../Time";
import { ProgressBar } from "../ui";
import ActionButton from "../ui/ActionButton";
import Banner from "./Banner";

type Props = {
  readonly dag?: DAGResponse | undefined;
};

const BackfillBanner = ({ dag }: Props) => {
  const { data, error } = useBackfillServiceListBackfills({
    dagId: dag?.dag_id,
  });

  // Need to change this based on data containing values
  const initialVisibility = error === null;
  const [visible, setVisible] = useState<boolean>(initialVisibility);

  const leftChildren = [
    <Text key="backfill">Backfill in progress:</Text>,
    <>
      <Time datetime={data?.backfills[0]?.from_date} /> - <Time datetime={data?.backfills[0]?.to_date} />
    </>,
  ];

  // Need to use proper keys, used simple values for convenience now
  const rightChildren = [
    <ProgressBar key="progressbar" size="xs" visibility="visible" />,
    <ActionButton
      actionName=""
      icon={<MdPause color="white" />}
      key="pause"
      rounded="full"
      text=""
      variant="outline"
    />,
    <ActionButton
      actionName=""
      icon={<MdStop color="white" />}
      key="stop"
      rounded="full"
      text=""
      variant="outline"
    />,
    <ActionButton
      actionName=""
      icon={<MdClose color="white" />}
      key="close"
      onClick={() => setVisible(false)}
      rounded="full"
      text=""
      variant="outline"
    />,
  ];

  return <Banner leftChildren={leftChildren} rightChildren={rightChildren} visible={visible} />;
};

export default BackfillBanner;
