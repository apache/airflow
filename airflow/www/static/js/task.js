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

/* global document, moment */

import validator from "validator";

// reformat task details to be more human-readable
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".js-ti-attr").forEach((attr) => {
    const value = attr.innerHTML;
    if (value.length === 32 && moment(value, "YYYY-MM-DD").isValid()) {
      // 32 is the length of our timestamps
      // eslint-disable-next-line no-param-reassign
      attr.innerHTML = "";
      const timeElement = document.createElement("time");
      timeElement.setAttribute("datetime", value);
      const textNode = document.createTextNode(value);
      timeElement.appendChild(textNode);
      attr.appendChild(timeElement);
    } else if (validator.isURL(value)) {
      // very basic url detection
      // eslint-disable-next-line no-param-reassign
      attr.innerHTML = "";
      const linkElement = document.createElement("a");
      linkElement.setAttribute("href", value);
      const textNode = document.createTextNode(value);
      linkElement.appendChild(textNode);
      attr.appendChild(linkElement);
    }
  });
});
