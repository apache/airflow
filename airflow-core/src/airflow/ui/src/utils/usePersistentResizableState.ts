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
import { useCallback, useState } from "react";

type Size = { height: number; width: number };

const getInitialSize = (key: string, defaultSize: Size): Size => {
  try {
    const item = localStorage.getItem(key);

    if (item === null) {
      return defaultSize;
    }

    const parsed: unknown = JSON.parse(item);

    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "width" in parsed &&
      "height" in parsed &&
      typeof parsed.width === "number" &&
      typeof parsed.height === "number"
    ) {
      return { height: parsed.height, width: parsed.width };
    }
  } catch {
    // Ignore parsing errors
  }

  return defaultSize;
};

export const usePersistentResizableState = (storageKey: string, defaultSize: Size) => {
  const [size, setSize] = useState(() => getInitialSize(storageKey, defaultSize));

  const handleResize = useCallback((_event: React.SyntheticEvent, { size: newSize }: { size: Size }) => {
    setSize(newSize);
  }, []);

  const handleResizeStop = useCallback(
    (_event: React.SyntheticEvent, { size: finalSize }: { size: Size }) => {
      try {
        localStorage.setItem(storageKey, JSON.stringify(finalSize));
      } catch {
        // Ignore storage errors
      }
    },
    [storageKey],
  );

  return { handleResize, handleResizeStop, size };
};
