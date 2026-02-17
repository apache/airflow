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
import { createIcon } from "@chakra-ui/react";

/**
 * Clock with warning indicator icon for missed deadlines.
 * Visually distinct from the triangle FailedIcon.
 */
export const DeadlineIcon = createIcon({
  defaultProps: {
    height: "1em",
    width: "1em",
  },
  displayName: "Deadline Icon",
  path: (
    <g fill="currentColor">
      <path
        clipRule="evenodd"
        d="M8 1a7 7 0 1 0 0 14A7 7 0 0 0 8 1ZM2.5 8a5.5 5.5 0 1 1 11 0 5.5 5.5 0 0 1-11 0Z"
        fillRule="evenodd"
      />
      <path d="M8 3.75a.75.75 0 0 1 .75.75v3.69l2.28 2.28a.75.75 0 1 1-1.06 1.06l-2.5-2.5A.75.75 0 0 1 7.25 8.5V4.5A.75.75 0 0 1 8 3.75Z" />
    </g>
  ),
  viewBox: "0 0 16 16",
});
