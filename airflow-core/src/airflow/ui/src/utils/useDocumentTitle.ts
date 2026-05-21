/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useEffect } from "react";

const BASE_TITLE = "Airflow";

/**
 * Hook to set the browser tab title dynamically.
 * When a pageTitle is provided, the document title becomes "{pageTitle} - Airflow".
 * When pageTitle is undefined/null, the document title resets to "Airflow".
 * On cleanup (unmount), the title is reset to the base title.
 */
export const useDocumentTitle = (pageTitle?: string | null) => {
  useEffect(() => {
    document.title = pageTitle ? `${pageTitle} - ${BASE_TITLE}` : BASE_TITLE;
    return () => {
      document.title = BASE_TITLE;
    };
  }, [pageTitle]);
};
