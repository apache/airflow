/**
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
export const defaultFormat = 'YYYY-MM-DD, HH:mm:ss';
export const defaultFormatWithTZ = 'YYYY-MM-DD, HH:mm:ss z';


export const formatDateTime = (datetime) => {
  return moment(datetime).format(defaultFormatWithTZ)
}

export const converAndFormatUTC = (datetime, tz) => {
  let dateTimeObj = moment.utc(datetime);
  if (tz) dateTimeObj = dateTimeObj.tz(tz);
  return dateTimeObj.format(defaultFormatWithTZ)
}

export const secondsToString = (seconds) => {
  let numdays    = Math.floor((seconds % 31536000) / 86400);
  let numhours   = Math.floor(((seconds % 31536000) % 86400) / 3600);
  let numminutes = Math.floor((((seconds % 31536000) % 86400) % 3600) / 60);
  let numseconds = Math.floor((((seconds % 31536000) % 86400) % 3600) % 60);
  return (numdays > 0    ? numdays    + (numdays    === 1 ? " day "    : " days ")    : "") +
         (numhours > 0   ? numhours   + (numhours   === 1 ? " hour "   : " hours ")   : "") +
         (numminutes > 0 ? numminutes + (numminutes === 1 ? " minute " : " minutes ") : "") +
         (numseconds > 0 ? numseconds + (numseconds === 1 ? " second"  : " seconds")  : "");
}
