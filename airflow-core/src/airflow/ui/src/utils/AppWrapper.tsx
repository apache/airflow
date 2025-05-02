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
import { RouterProvider, createMemoryRouter } from "react-router-dom";

import { TimezoneProvider } from "src/context/timezone";
import { routerConfig } from "src/router";
import { BaseWrapper } from "src/utils/Wrapper";

type Props = {
  readonly initialEntries: Array<string>;
};

export const AppWrapper = ({ initialEntries }: Props) => {
  const router = createMemoryRouter(routerConfig, { basename: "/", initialEntries });

  return (
    <BaseWrapper>
      <TimezoneProvider>
        <RouterProvider router={router} />
      </TimezoneProvider>
    </BaseWrapper>
  );
};
