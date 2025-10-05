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
import { Link as ExternalLink } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

type Props = {
  readonly children: React.ReactNode;
  readonly inPlugin?: boolean;
  readonly to: string;
};

export const Link = ({ children, inPlugin, to }: Props) => {
  // Need to check whether ReactRouterDOM is available globally
  // because in Airflow 3.1.0, the plugin system was missing this.
  if (inPlugin || (globalThis as Record<string, unknown>).ReactRouterDOM) {
    return <RouterLink to={to}>{children}</RouterLink>;
    // TODO need to fix internal URL in plugin... in 3.1.0
  } else {
    // Fallback in 3.1.0, can be removed if we drop support for it
    return <ExternalLink href={`..${to}`}>{children}</ExternalLink>;
  }
};
