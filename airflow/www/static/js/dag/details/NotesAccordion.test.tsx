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

/* global describe, test, expect */

import React from 'react';
import { render, fireEvent, waitFor } from '@testing-library/react';

import * as utils from 'src/utils';
import { Wrapper } from 'src/utils/testUtils';

import NotesAccordion from './NotesAccordion';

describe('Test DagRun / Task Instance Notes', () => {
  window.scrollTo = jest.fn();

  afterEach(() => {
    jest.resetAllMocks();
  });

  afterAll(() => {
    jest.clearAllMocks();
  });

  test('No initial value, accordion is also open', async () => {
    jest.spyOn(utils, 'getMetaValue').mockImplementation(
      (meta) => {
        if (meta === 'can_edit') return 'True';
        return '';
      },
    );

    const { getByText } = render(
      <NotesAccordion dagId="dagId" runId="runId" />,
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(getByText('Add Note')).toBeVisible());
  });

  test('With initial value, accordion is open. And update button changed', () => {
    jest.spyOn(utils, 'getMetaValue').mockImplementation(
      (meta) => {
        if (meta === 'can_edit') return 'True';
        return '';
      },
    );

    const { queryByText, getByText } = render(
      <NotesAccordion dagId="dagId" runId="runId" initialValue="I am a note" />,
      { wrapper: Wrapper },
    );

    const changeButton = getByText('Edit Note');

    expect(changeButton).toBeInTheDocument();
    expect(queryByText('Add Note')).toBe(null);

    fireEvent.click(changeButton);

    expect(getByText('Save Note')).toBeInTheDocument();
    expect(getByText('Cancel')).toBeInTheDocument();
  });

  test('Cannot Edit Note without edit permissions', () => {
    jest.spyOn(utils, 'getMetaValue').mockImplementation(
      (meta) => {
        if (meta === 'can_edit') return 'False';
        return '';
      },
    );

    const { getByText } = render(
      <NotesAccordion dagId="dagId" runId="runId" initialValue="I am a note" />,
      { wrapper: Wrapper },
    );

    const changeButton = getByText('Edit Note');

    expect(changeButton).toBeInTheDocument();
    expect(changeButton).toBeDisabled();
  });

  test('Making changes and then discarding will go reset to the original notes.', () => {
    jest.spyOn(utils, 'getMetaValue').mockImplementation(
      (meta) => {
        if (meta === 'can_edit') return 'True';
        return '';
      },
    );

    const { getByTestId, getByText, queryByText } = render(
      <NotesAccordion dagId="dagId" runId="runId" initialValue="I am a note" />,
      { wrapper: Wrapper },
    );

    const changeButton = getByText('Edit Note');

    fireEvent.click(changeButton);

    expect(getByText('Save Note')).toBeInTheDocument();
    const textarea = getByTestId('notes-input');

    fireEvent.change(textarea, { target: { value: 'A different note.' } });

    expect(queryByText('I am a note')).toBe(null);

    fireEvent.click(getByText('Cancel'));

    expect(getByText('I am a note')).toBeInTheDocument();
  });
});
