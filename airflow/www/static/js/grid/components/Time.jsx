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

/* global moment */

import React from 'react';
import { useTimezone } from '../context/timezone';
import { defaultFormatWithTZ } from '../../datetime_utils';

const Time = ({ dateTime, format = defaultFormatWithTZ }) => {
  const { timezone } = useTimezone();
  const time = moment(dateTime);

  // eslint-disable-next-line no-underscore-dangle
  if (!dateTime || !time._isValid) return null;

  const formattedTime = time.tz(timezone).format(format);
  const utcTime = time.tz('UTC').format(defaultFormatWithTZ);

  return (
    <time
      dateTime={dateTime}
      // show title if date is not UTC
      title={timezone.toUpperCase() !== 'UTC' ? utcTime : undefined}
    >
      {formattedTime}
    </time>
  );
};

export default Time;
