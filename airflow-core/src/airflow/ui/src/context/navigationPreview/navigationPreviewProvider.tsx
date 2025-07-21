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
import { useCallback, useMemo, useState, type PropsWithChildren } from "react";

import { NavigationPreviewContext, type NavigationPreviewContextType } from "./Context";

export const NavigationPreviewProvider = ({ children }: PropsWithChildren) => {
  const [previewTaskId, setPreviewTaskId] = useState<string | undefined>(undefined);
  const [previewRunId, setPreviewRunId] = useState<string | undefined>(undefined);

  const setPreview = useCallback((taskId: string | undefined, runId: string | undefined) => {
    setPreviewTaskId(taskId);
    setPreviewRunId(runId);
  }, []);

  const clearPreview = useCallback(() => {
    setPreviewTaskId(undefined);
    setPreviewRunId(undefined);
  }, []);

  const value = useMemo<NavigationPreviewContextType>(
    () => ({
      clearPreview,
      previewRunId,
      previewTaskId,
      setPreview,
    }),
    [clearPreview, previewRunId, previewTaskId, setPreview]
  );

  return (
    <NavigationPreviewContext.Provider value={value}>
      {children}
    </NavigationPreviewContext.Provider>
  );
};
