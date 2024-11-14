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

import { useToast } from "@chakra-ui/react";
import type { ReactNode } from "react";

import handleError from "./handleError";

const useErrorToast = () => {
  const toast = useToast();
  // Add an error prop and handle it as a description
  return ({ error, title, ...rest }: { error: Error; title?: ReactNode }) => {
    toast({
      ...rest,
      status: "error",
      title: title || "Error",
      description: handleError(error).slice(0, 500),
    });
  };
};

export default useErrorToast;
