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
 * Warning/error icon (triangle with exclamation) for failed state.
 * Use in UI components; for Canvas (e.g. Chart.js) use canvas drawing or an image.
 */
export const FailedIcon = createIcon({
  defaultProps: {
    height: "1em",
    width: "1em",
  },
  displayName: "Failed Icon",
  path: (
    <g fill="currentColor">
      <path
        clipRule="evenodd"
        d="M8 1.5a1 1 0 0 1 .87.51l6.5 11a1 1 0 0 1-.87 1.49H1.5a1 1 0 0 1-.87-1.49l6.5-11A1 1 0 0 1 8 1.5ZM8 4a.75.75 0 0 0-.75.75v3.5a.75.75 0 0 0 1.5 0v-3.5A.75.75 0 0 0 8 4Zm0 8a.75.75 0 1 0 0-1.5.75.75 0 0 0 0 1.5Z"
        fillRule="evenodd"
      />
    </g>
  ),
  viewBox: "0 0 16 16",
});
