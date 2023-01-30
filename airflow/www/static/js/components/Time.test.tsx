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

/* global describe, test, expect, document, CustomEvent */

import React from 'react';
import {
  render, fireEvent, act,
} from '@testing-library/react';
import moment from 'moment-timezone';

import { defaultFormatWithTZ, TimezoneEvent } from 'src/datetime_utils';
import { Wrapper } from 'src/utils/testUtils';

import Time from './Time';

describe('Test Time and TimezoneProvider', () => {
  test('Displays a UTC time correctly', () => {
    const now = new Date();
    const { getByText } = render(
      <Time dateTime={now.toISOString()} />,
      { wrapper: Wrapper },
    );

    const utcTime = getByText(moment.utc(now).format(defaultFormatWithTZ));
    expect(utcTime).toBeDefined();
    expect(utcTime.title).toBeFalsy();
  });

  test('Displays moment default tz, includes UTC date in title', () => {
    const now = new Date();
    const tz = 'US/Samoa';
    moment.tz.setDefault(tz);

    const { getByText } = render(
      <Time dateTime={now.toISOString()} />,
      { wrapper: Wrapper },
    );

    const samoaTime = getByText(moment(now).tz(tz).format(defaultFormatWithTZ));
    expect(samoaTime).toBeDefined();
    expect(samoaTime.title).toEqual(moment.utc(now).format(defaultFormatWithTZ));
  });

  test('Updates based on timezone change', async () => {
    const now = new Date();
    const { getByText, queryByText } = render(
      <Time dateTime={now.toISOString()} />,
      { wrapper: Wrapper },
    );

    const utcTime = queryByText(moment.utc(now).format(defaultFormatWithTZ));
    expect(utcTime).toBeDefined();

    // Fire a custom timezone change event
    const event = new CustomEvent(TimezoneEvent, {
      detail: 'EST',
    });
    await act(async () => {
      fireEvent(document, event);
    });

    expect(utcTime).toBeNull();
    const estTime = getByText(moment(now).tz('EST').format(defaultFormatWithTZ));
    expect(estTime).toBeDefined();
    expect(estTime.title).toEqual(moment.utc(now).format(defaultFormatWithTZ));
  });
});
