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

import React, { useState, useMemo } from 'react';
import {
  Flex,
  Text,
  Box,
  Link,
  IconButton,
} from '@chakra-ui/react';
import { snakeCase } from 'lodash';
import {
  MdDetails, MdCode, MdSyncAlt, MdReorder,
} from 'react-icons/md';

import { getMetaValue } from '../../../../utils';
import { formatDuration, getDuration } from '../../../../datetime_utils';
import { useMappedInstances } from '../../../api';
import { SimpleStatus } from '../../../components/StatusBox';
import Table from '../../../components/Table';
import Time from '../../../components/Time';

const canEdit = getMetaValue('can_edit') === 'True';
const renderedTemplatesUrl = getMetaValue('rendered_templates_url');
const logUrl = getMetaValue('log_url');
const taskUrl = getMetaValue('task_url');
const xcomUrl = getMetaValue('xcom_url');

const IconLink = (props) => (
  <IconButton as={Link} variant="ghost" colorScheme="blue" fontSize="3xl" {...props} />
);

const MappedInstances = ({
  dagId, runId, taskId, selectRows,
}) => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState([]);

  const sort = sortBy[0];

  const order = sort && (sort.id === 'state' || sort.id === 'mapIndex') ? `${sort.desc ? '-' : ''}${snakeCase(sort.id)}` : '';

  const {
    data: { taskInstances, totalEntries } = { taskInstances: [], totalEntries: 0 },
    isLoading,
  } = useMappedInstances({
    dagId, runId, taskId, limit, offset, order,
  });

  const data = useMemo(
    () => taskInstances.map((mi) => {
      const params = new URLSearchParams({
        dag_id: dagId,
        task_id: mi.taskId,
        execution_date: mi.executionDate,
        map_index: mi.mapIndex,
      }).toString();
      const detailsLink = `${taskUrl}&${params}`;
      const renderedLink = `${renderedTemplatesUrl}&${params}`;
      const logLink = `${logUrl}&${params}`;
      const xcomLink = `${xcomUrl}&${params}`;
      return {
        ...mi,
        state: (
          <Flex alignItems="center">
            <SimpleStatus state={mi.state} mx={2} />
            {mi.state || 'no status'}
          </Flex>
        ),
        duration: mi.duration && formatDuration(getDuration(mi.startDate, mi.endDate)),
        startDate: <Time dateTime={mi.startDate} />,
        endDate: <Time dateTime={mi.endDate} />,
        links: (
          <Flex alignItems="center">
            <IconLink mr={1} title="Details" aria-label="Details" icon={<MdDetails />} href={detailsLink} />
            <IconLink mr={1} title="Rendered Templates" aria-label="Rendered Templates" icon={<MdCode />} href={renderedLink} />
            <IconLink mr={1} title="Log" aria-label="Log" icon={<MdReorder />} href={logLink} />
            <IconLink title="XCom" fontWeight="bold" aria-label="XCom" icon={<MdSyncAlt />} href={xcomLink} />
          </Flex>
        ),
      };
    }),
    [dagId, taskInstances],
  );

  const columns = useMemo(
    () => [
      {
        Header: 'Map Index',
        accessor: 'mapIndex',
      },
      {
        Header: 'State',
        accessor: 'state',
      },
      {
        Header: 'Duration',
        accessor: 'duration',
        disableSortBy: true,
      },
      {
        Header: 'Start Date',
        accessor: 'startDate',
        disableSortBy: true,
      },
      {
        Header: 'End Date',
        accessor: 'endDate',
        disableSortBy: true,
      },
      {
        disableSortBy: true,
        accessor: 'links',
      },
    ],
    [],
  );

  return (
    <Box>
      <br />
      <Text as="strong">Mapped Instances</Text>
      <Table
        data={data}
        columns={columns}
        manualPagination={{
          offset,
          setOffset,
          totalEntries,
        }}
        pageSize={limit}
        setSortBy={setSortBy}
        isLoading={isLoading}
        selectRows={canEdit && selectRows}
      />
    </Box>
  );
};

export default MappedInstances;
