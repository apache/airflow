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

export const TaskIcon = createIcon({
  defaultProps: {
    height: "1em",
    width: "1em",
  },
  displayName: "Task Icon",
  path: (
    <g clipPath="url(#a)" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2">
      <path d="m7.99967 10.6666c1.47276 0 2.66663-1.19392 2.66663-2.66668s-1.19387-2.66667-2.66663-2.66667c-1.47275 0-2.66666 1.19391-2.66666 2.66667s1.19391 2.66668 2.66666 2.66668z" />
      <path d="m.700195 8h3.966665" />
      <path d="m11.3398 8h3.9667" />
    </g>
  ),
  viewBox: "0 0 16 16",
});
