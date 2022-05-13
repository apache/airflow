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
import '@testing-library/jest-dom';
import { render, fireEvent, within } from '@testing-library/react';
import { sortBy } from 'lodash';

import Table from './Table';
import { ChakraWrapper } from '../utils/testUtils';

const data = [
  { firstName: 'Lamont', lastName: 'Grimes', country: 'United States' },
  { firstName: 'Alysa', lastName: 'Armstrong', country: 'Spain' },
  { firstName: 'Petra', lastName: 'Blick', country: 'France' },
  { firstName: 'Jeromy', lastName: 'Herman', country: 'Mexico' },
  { firstName: 'Eleonore', lastName: 'Rohan', country: 'Nigeria' },
];

const columns = [
  {
    Header: 'First Name',
    accessor: 'firstName',
  },
  {
    Header: 'Last Name',
    accessor: 'lastName',
  },
  {
    Header: 'Country',
    accessor: 'country',
  },
];

describe('Test Table', () => {
  test('Displays correct data', async () => {
    const { getAllByRole, getByText, queryByTitle } = render(
      <Table data={data} columns={columns} />,
      { wrapper: ChakraWrapper },
    );

    const rows = getAllByRole('row');
    const name1 = getByText(data[0].firstName);
    const name2 = getByText(data[1].firstName);
    const name3 = getByText(data[2].firstName);
    const name4 = getByText(data[3].firstName);
    const name5 = getByText(data[4].firstName);
    const previous = queryByTitle('Previous Page');
    const next = queryByTitle('Next Page');

    // table header is a row so add 1 to expected amount
    expect(rows).toHaveLength(6);

    expect(name1).toBeInTheDocument();
    expect(name2).toBeInTheDocument();
    expect(name3).toBeInTheDocument();
    expect(name4).toBeInTheDocument();
    expect(name5).toBeInTheDocument();

    // expect pagination to de hidden when fewer results than default pageSize (24)
    expect(previous).toBeNull();
    expect(next).toBeNull();
  });

  test('Shows empty state', async () => {
    const { getAllByRole, getByText } = render(
      <Table data={[]} columns={columns} />,
      { wrapper: ChakraWrapper },
    );

    const rows = getAllByRole('row');

    // table header is a row so add 1 to expected amount
    expect(rows).toHaveLength(2);
    expect(getByText('No Data found.')).toBeInTheDocument();
  });

  test('With pagination', async () => {
    const { getAllByRole, queryByText, getByTitle } = render(
      <Table data={data} columns={columns} pageSize={2} />,
      { wrapper: ChakraWrapper },
    );

    const name1 = data[0].firstName;
    const name2 = data[1].firstName;
    const name3 = data[2].firstName;
    const name4 = data[3].firstName;
    const name5 = data[4].firstName;
    const previous = getByTitle('Previous Page');
    const next = getByTitle('Next Page');

    /// // PAGE ONE // ///
    // table header is a row so add 1 to expected amount
    expect(getAllByRole('row')).toHaveLength(3);

    expect(queryByText(name1)).toBeInTheDocument();
    expect(queryByText(name2)).toBeInTheDocument();
    expect(queryByText(name3)).toBeNull();
    expect(queryByText(name4)).toBeNull();
    expect(queryByText(name5)).toBeNull();

    // expect only pagination next button to be functional on 1st page
    expect(previous).toBeDisabled();
    expect(next).toBeEnabled();

    fireEvent.click(next);

    /// // PAGE TWO // ///
    expect(getAllByRole('row')).toHaveLength(3);

    expect(queryByText(name1)).toBeNull();
    expect(queryByText(name2)).toBeNull();
    expect(queryByText(name3)).toBeInTheDocument();
    expect(queryByText(name4)).toBeInTheDocument();
    expect(queryByText(name5)).toBeNull();

    // expect both pagination buttons to be functional on 2nd page
    expect(previous).toBeEnabled();
    expect(next).toBeEnabled();

    fireEvent.click(next);

    /// // PAGE THREE // ///
    expect(getAllByRole('row')).toHaveLength(2);

    expect(queryByText(name1)).toBeNull();
    expect(queryByText(name2)).toBeNull();
    expect(queryByText(name3)).toBeNull();
    expect(queryByText(name4)).toBeNull();
    expect(queryByText(name5)).toBeInTheDocument();

    // expect only pagination previous button to be functional on last page
    expect(previous).toBeEnabled();
    expect(next).toBeDisabled();
  });

  test('With sorting', async () => {
    const { getAllByRole } = render(<Table data={data} columns={columns} />, {
      wrapper: ChakraWrapper,
    });

    // Default order matches original data order //
    const firstNameHeader = getAllByRole('columnheader')[0];
    const rows = getAllByRole('row');

    const firstRowName = within(rows[1]).queryByText(data[0].firstName);
    const lastRowName = within(rows[5]).queryByText(data[4].firstName);
    expect(firstRowName).toBeInTheDocument();
    expect(lastRowName).toBeInTheDocument();

    fireEvent.click(firstNameHeader);

    /// // ASCENDING SORT // ///
    const ascendingRows = getAllByRole('row');
    const ascendingData = sortBy(data, [(o) => o.firstName]);

    const ascendingFirstRowName = within(ascendingRows[1]).queryByText(ascendingData[0].firstName);
    const ascendingLastRowName = within(ascendingRows[5]).queryByText(ascendingData[4].firstName);
    expect(ascendingFirstRowName).toBeInTheDocument();
    expect(ascendingLastRowName).toBeInTheDocument();

    fireEvent.click(firstNameHeader);

    /// // DESCENDING SORT // ///
    const descendingRows = getAllByRole('row');
    const descendingData = sortBy(data, [(o) => o.firstName]).reverse();

    const descendingFirstRowName = within(descendingRows[1]).queryByText(
      descendingData[0].firstName,
    );
    const descendingLastRowName = within(descendingRows[5]).queryByText(
      descendingData[4].firstName,
    );
    expect(descendingFirstRowName).toBeInTheDocument();
    expect(descendingLastRowName).toBeInTheDocument();
  });

  test('Shows checkboxes', async () => {
    const { getAllByTitle } = render(
      <Table data={data} columns={columns} selectRows={() => {}} />,
      { wrapper: ChakraWrapper },
    );

    const checkboxes = getAllByTitle('Toggle Row Selected');
    expect(checkboxes).toHaveLength(data.length);

    const checkbox1 = checkboxes[1];

    fireEvent.click(checkbox1);

    expect(checkbox1).toHaveAttribute('data-checked');
  });
});
