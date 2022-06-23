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

import React from 'react';
import '@testing-library/jest-dom';
import { render, fireEvent } from '@testing-library/react';

import TimezoneDropdown from 'components/AppContainer/TimezoneDropdown';
import DateProvider from 'providers/DateProvider';
import { ChakraWrapper } from './utils';

describe('test timezone dropdown', () => {
  test('Can search for a new timezone and the date changes', () => {
    const { getByText, queryByText } = render(
      <DateProvider>
        <TimezoneDropdown />
      </DateProvider>,
      { wrapper: ChakraWrapper },
    );

    const initialTime = '+00:00';

    expect(getByText(initialTime, { exact: false })).toBeInTheDocument();
    const button = getByText(initialTime, { exact: false });
    fireEvent.click(button);
    const focusedElement = document.activeElement;
    if (focusedElement) {
      fireEvent.change(focusedElement, { target: { value: 'Baku' } });
    }
    const optionText = '+04:00 Asia/Baku';
    const option = getByText(optionText);
    expect(option).toBeInTheDocument();
    fireEvent.click(option);

    expect(queryByText(optionText)).toBeNull();
    expect(getByText('+04:00', { exact: false })).toBeInTheDocument();
  });
});
