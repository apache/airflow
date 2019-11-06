/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export const moment = require('moment-timezone');
export const defaultFormat = 'YYYY-MM-DD, HH:mm:ss';
export const defaultFormatWithTZ = 'YYYY-MM-DD, HH:mm:ss z';


const makeDateTimeHTML = (start, end) => {
  return (
    `Started: ${start.format(defaultFormat)} <br> Ended: ${end.format(defaultFormat)} <br>`
  )
};

export const generateTooltipDateTime = (startDate, endDate, dagTZ) => {
  const tzFormat = 'z (Z)';
  const localTZ = moment.tz.guess();
  startDate = moment.utc(startDate);
  endDate = moment.utc(endDate);
  dagTZ = dagTZ.toUpperCase();

  // Generate UTC Start and End Date
  let tooltipHTML = '<br><strong>UTC</strong><br>';
  tooltipHTML += makeDateTimeHTML(startDate, endDate);

  // Generate User's Local Start and End Date
  tooltipHTML += `<br><strong>Local: ${moment.tz(localTZ).format(tzFormat)}</strong><br>`;
  tooltipHTML += makeDateTimeHTML(startDate.local(), endDate.local());

  // Generate DAG's Start and End Date
  if (dagTZ !== 'UTC' && dagTZ !== localTZ) {
    tooltipHTML += `<br><strong>DAG's TZ: ${moment.tz(dagTZ).format(tzFormat)}</strong><br>`;
    tooltipHTML += makeDateTimeHTML(startDate.tz(dagTZ), endDate.tz(dagTZ));
  }

  return tooltipHTML
};


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