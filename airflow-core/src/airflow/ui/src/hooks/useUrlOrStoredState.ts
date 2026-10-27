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
import { useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

type Options<T> = {
  defaultValue: T;
  // Value carried by the URL, or `undefined` when the URL doesn't specify one.
  readParams: (params: URLSearchParams) => T | undefined;
  replace?: boolean;
  storageKey: string;
  // Mutates `params` so it carries (or drops) `value`.
  writeParams: (params: URLSearchParams, value: T) => void;
};

// A value mirrored between the URL and localStorage. An explicit URL value wins so a shared link
// reproduces the sender's choice, otherwise the remembered localStorage value applies. Setting
// writes both, with `writeParams` deciding what (if anything) the URL keeps.
export const useUrlOrStoredState = <T>({
  defaultValue,
  readParams,
  replace,
  storageKey,
  writeParams,
}: Options<T>) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [storedValue, setStoredValue] = useLocalStorage<T>(storageKey, defaultValue);

  const value = readParams(searchParams) ?? storedValue;

  const setValue = (nextValue: T) => {
    setStoredValue(nextValue);
    setSearchParams(
      (previous) => {
        const next = new URLSearchParams(previous);

        writeParams(next, nextValue);

        return next;
      },
      { replace },
    );
  };

  return [value, setValue] as const;
};
