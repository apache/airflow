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
import {
  Code, Link,
} from '@chakra-ui/react';

import Time from 'src/components/Time';

export const TimeCell = ({ cell: { value } }: any) => <Time dateTime={value} />;

export const DatasetLink = ({ cell: { value } }: any) => <Link color="blue.600" href={`/datasets?dataset_id=${value}`}>{value}</Link>;

export const TaskInstanceLink = ({ cell: { value, row } }: any) => {
  const { sourceRunId, sourceDagId, sourceMapIndex } = row.original;
  const url = `/dags/${sourceDagId}/grid?dag_run_id=${encodeURIComponent(sourceRunId)}&task_id=${encodeURIComponent(value)}`;
  const mapIndex = sourceMapIndex > -1 ? `[${sourceMapIndex}]` : '';
  return (<Link color="blue.600" href={url}>{`${sourceDagId}.${value}${mapIndex}`}</Link>);
};

export const CodeCell = ({ cell: { value } }: any) => <Code>{value}</Code>;
