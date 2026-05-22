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
import { createContext, useContext, useEffect, useMemo, useState, type PropsWithChildren } from "react";

import { useConfig } from "src/queries/useConfig";

type DocumentTitleContextValue = {
  readonly setPageTitle: (title: string | undefined) => void;
};

const DocumentTitleContext = createContext<DocumentTitleContextValue | undefined>(undefined);

const getTitlePart = (title: unknown) => (typeof title === "string" && title.length > 0 ? title : undefined);

export const DocumentTitleProvider = ({ children }: PropsWithChildren) => {
  const instanceName = getTitlePart(useConfig("instance_name"));
  const [pageTitle, setPageTitle] = useState<string | undefined>();
  const value = useMemo(() => ({ setPageTitle }), []);

  useEffect(() => {
    const title = [pageTitle, instanceName].filter(Boolean).join(" - ");

    if (title.length > 0) {
      document.title = title;
    }
  }, [instanceName, pageTitle]);

  return <DocumentTitleContext.Provider value={value}>{children}</DocumentTitleContext.Provider>;
};

export const useDocumentTitle = (title: string | undefined) => {
  const context = useContext(DocumentTitleContext);

  if (context === undefined) {
    throw new Error("useDocumentTitle must be used within DocumentTitleProvider");
  }

  useEffect(() => {
    context.setPageTitle(title);

    return () => context.setPageTitle(undefined);
  }, [context, title]);
};
