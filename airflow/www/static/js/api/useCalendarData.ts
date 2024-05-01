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

import { useQuery } from "react-query";
import axios, { AxiosResponse } from "axios";

import { getMetaValue } from "src/utils";

const DAG_ID_PARAM = "dag_id";

const dagId = getMetaValue(DAG_ID_PARAM);
const calendarDataUrl = getMetaValue("calendar_data_url");

interface DagState {
  count: number;
  date: string;
  state: string;
}

interface CalendarData {
  dagStates: DagState[];
}

const useCalendarData = () =>
  useQuery(["calendarData"], async () => {
    const params = {
      [DAG_ID_PARAM]: dagId,
    };
    return axios.get<AxiosResponse, CalendarData>(calendarDataUrl, { params });
  });

export default useCalendarData;
