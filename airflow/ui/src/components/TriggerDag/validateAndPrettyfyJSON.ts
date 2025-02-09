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
export const validateAndPrettifyJson = (
  value: string,
  conf: string,
  setConf: (confString: string) => void,
  setErrors: React.Dispatch<
    React.SetStateAction<{
      conf?: string;
      date?: unknown;
    }>
  >,
  // eslint-disable-next-line @typescript-eslint/max-params
) => {
  try {
    const parsedJson = JSON.parse(value) as JSON;

    setErrors((prev) => ({ ...prev, conf: undefined }));

    const formattedJson = JSON.stringify(parsedJson, undefined, 2);

    if (formattedJson !== conf) {
      setConf(formattedJson); // Update only if the value is different
    }

    return formattedJson;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Unknown error occurred.";

    setErrors((prev) => ({
      ...prev,
      conf: `Invalid JSON format: ${errorMessage}`,
    }));

    return value;
  }
};
