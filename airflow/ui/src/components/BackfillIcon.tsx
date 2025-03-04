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

export const BackfillIcon = createIcon({
  defaultProps: {
    height: "1.5em",
    width: "1.5em",
  },
  displayName: "Backfill",
  path: (
    <path
      clipRule="evenodd"
      d="M20.5 3.5v17a1 1 0 0 1-1 1h-3.278c-.562 0-.722-.541-.722-1.25V3.75c0-.709.16-1.25.722-1.25H19.5a1 1 0 0 1 1 1Zm-6 0v17a1 1 0 0 1-1 1h-3.278c-.562 0-.722-.541-.722-1.25V3.75c0-.709.16-1.25.722-1.25H13.5a1 1 0 0 1 1 1Zm-6 16.1v-1h-1v1h1Zm-1 .9h-.003v1H7.5a1 1 0 0 0 .995-.9H7.5v-.1Zm-1.003 1v-1h-1v1h1Zm-2 0v-1h-.99c.038.582.222 1 .715 1h.275ZM3.5 19.507h1v-.994h-1v.994Zm0-1.987h1v-.994h-1v.994Zm0-1.987h1v-.993h-1v.993Zm0-1.986h1v-.994h-1v.994Zm0-1.987h1v-.994h-1v.994Zm0-1.987h1V8.58h-1v.993Zm0-1.986h1v-.994h-1v.994Zm0-1.987h1v-.907h-1V5.6Zm0-1.9h1v-.2h-.003v-1h-.275c-.548 0-.714.516-.722 1.2Zm1.997-1.2v1h1v-1h-1Zm2 0v1H7.5v.1h1v-.1a1 1 0 0 0-1-1h-.003ZM8.5 4.6h-1v1h1v-1Zm0 2h-1v1h1v-1Zm0 2h-1v1h1v-1Zm0 2h-1v1h1v-1Zm0 2h-1v1h1v-1Zm0 2h-1v1h1v-1Zm0 2h-1v1h1v-1Z"
      fill="currentColor"
      fillRule="evenodd"
    />
  ),
  viewBox: "0 0 24 24",
});
