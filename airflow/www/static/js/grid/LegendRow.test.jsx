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

/* global describe, test, expect, stateColors, jest */

import React from 'react';
import { render, fireEvent } from '@testing-library/react';

import LegendRow from './LegendRow';

describe('Test LegendRow', () => {
  test('Render displays correctly the different task states', () => {
    const { getByText } = render(
      <LegendRow />,
    );

    Object.keys(stateColors).forEach((taskState) => {
      expect(getByText(taskState)).toBeInTheDocument();
    });

    expect(getByText('no_status')).toBeInTheDocument();
  });

  test.each([
    { state: 'success', expectedSetValue: 'success' },
    { state: 'failed', expectedSetValue: 'failed' },
    { state: 'no_status', expectedSetValue: null },
  ])('Hovering $state badge should trigger setHoverdTaskState function with $expectedSetValue',
    async ({ state, expectedSetValue }) => {
      const setHoveredTaskState = jest.fn();
      const { getByText } = render(
        <LegendRow setHoveredTaskState={setHoveredTaskState} />,
      );
      const successElement = getByText(state);
      fireEvent.mouseEnter(successElement);
      expect(setHoveredTaskState).toHaveBeenCalledWith(expectedSetValue);
      fireEvent.mouseLeave(successElement);
      expect(setHoveredTaskState).toHaveBeenLastCalledWith();
    });
});
