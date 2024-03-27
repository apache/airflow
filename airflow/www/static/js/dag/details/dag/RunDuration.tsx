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

import React, { useState } from "react";
import { Box, Checkbox, Flex } from "@chakra-ui/react";

import InfoTooltip from "src/components/InfoTooltip";
import RunDurationChart from "./RunDurationChart";

const LANDING_TIME_KEY = "showLandingTimes";

const RunDuration = () => {
  const storedValue = localStorage.getItem(LANDING_TIME_KEY);
  const [showLandingTimes, setShowLandingTimes] = useState(
    storedValue ? JSON.parse(storedValue) : true
  );
  const onChange = () => {
    localStorage.setItem(LANDING_TIME_KEY, (!showLandingTimes).toString());
    setShowLandingTimes(!showLandingTimes);
  };

  return (
    <Box height="50%">
      <Flex justifyContent="right" pr="30px">
        <Checkbox isChecked={showLandingTimes} onChange={onChange} size="lg">
          Show Landing Times
        </Checkbox>
        <InfoTooltip
          label="Landing Time is the difference between the Data Interval End and the
        start of the Dag Run"
          size={16}
        />
      </Flex>
      <RunDurationChart showLandingTimes={showLandingTimes} />
    </Box>
  );
};
export default RunDuration;
