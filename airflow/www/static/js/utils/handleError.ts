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

import axios from "axios";

const handleError = (error?: any, fallbackMessage?: string) => {
  if (typeof error === "string") return error;
  if (error?.response?.errors) {
    return error.response.errors.map((e: any) => e.message).join("\n");
  }
  if (axios.isAxiosError(error)) {
    return (
      error.response?.data?.detail || error.response?.data || error.message
    );
  }
  if (error?.message) return error.message;
  return fallbackMessage || "Something went wrong.";
};

export default handleError;
