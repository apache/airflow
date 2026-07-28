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
import { useEffect, type PropsWithChildren, useState } from "react";

import { useConfig } from "src/queries/useConfig";

import { DocumentTitleContext } from "./documentTitleContext";

const getDefaultTitle = (instanceConfig: unknown) =>
  typeof instanceConfig === "string" && instanceConfig.length > 0 ? instanceConfig : "Airflow";

export const DocumentTitleProvider = ({ children }: PropsWithChildren) => {
  const instanceConfig = useConfig("instance_name");
  const instanceName = getDefaultTitle(instanceConfig);
  const [pageTitle, setPageTitle] = useState<string | null>(null);

  useEffect(() => {
    document.title = pageTitle === null ? instanceName : `${pageTitle} - ${instanceName}`;
  }, [instanceName, pageTitle]);

  return <DocumentTitleContext.Provider value={setPageTitle}>{children}</DocumentTitleContext.Provider>;
};
