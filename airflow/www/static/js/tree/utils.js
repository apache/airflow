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

// moment will resolve the enddate to now if it is undefined
export const getDuration = (startDate, endDate) => (
  moment(endDate || undefined).diff(startDate || undefined)
);

export const formatDuration = (duration) => {
  const days = moment.duration(duration).days();
  // .as('milliseconds') is necessary for .format() to work correctly
  return `${days > 0 ? `${days}d` : ''}${moment.utc(moment.duration(duration).as('milliseconds')).format('HH:mm:ss')}`;
};
