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

import React, {
  createContext, useContext, useState, ReactNode, ReactElement,
} from 'react';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import tz from 'dayjs/plugin/timezone';

dayjs.extend(utc);
dayjs.extend(tz);

export const HOURS_24 = 'HH:mm Z';
export const HOURS_12 = 'h:mmA Z';

interface DateFormatContextData {
  dateFormat: string;
  setDateFormat: (value: string) => void;
  toggle24Hour: () => void;
}

export const DateFormatContext = createContext<DateFormatContextData>({
  dateFormat: HOURS_24,
  setDateFormat: () => {},
  toggle24Hour: () => {},
});

export const useDateFormatContext = () => useContext(DateFormatContext);

type Props = {
  children: ReactNode;
};

const DateFormatProvider = ({ children }: Props): ReactElement => {
  const [dateFormat, setDateFormat] = useState(HOURS_24);

  const toggle24Hour = () => {
    setDateFormat(dateFormat === HOURS_24 ? HOURS_12 : HOURS_24);
  };

  return (
    <DateFormatContext.Provider
      value={{
        dateFormat,
        setDateFormat,
        toggle24Hour,
      }}
    >
      {children}
    </DateFormatContext.Provider>
  );
};

export default DateFormatProvider;
