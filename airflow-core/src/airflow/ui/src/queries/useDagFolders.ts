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
import { useDagServiceGetDagFolders } from "openapi/queries";

/**
 * Fetch the distinct folders of all readable Dags, each paired with its bundle.
 *
 * The list powers the folder navigation tree on the Dags page; the tree hierarchy is
 * reconstructed client-side by splitting each path on ``/`` and grouping by bundle.
 */
export const useDagFolders = () => {
  const { data, error, isLoading } = useDagServiceGetDagFolders();

  return {
    error,
    folders: data?.folders ?? [],
    isLoading,
  };
};
