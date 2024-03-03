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

import { useCallback, useEffect, useLayoutEffect, useRef } from "react";
import { debounce } from "lodash";

import type { KeyboardShortcutKeys } from "src/types";

const isInputInFocus = "isInputInFocus";

const useKeysPress = (
  keyboardShortcutKeys: KeyboardShortcutKeys,
  callback: Function,
  node = null
) => {
  const callbackRef = useRef(callback);
  useLayoutEffect(() => {
    callbackRef.current = callback;
  });

  // handle what happens on key press
  const handleKeyPress = useCallback(
    (event: KeyboardEvent) => {
      // check if one of the primaryKey and secondaryKey are pressed at once
      if (
        !JSON.parse(localStorage.getItem(isInputInFocus) || "true") &&
        event[keyboardShortcutKeys.primaryKey] &&
        keyboardShortcutKeys.secondaryKey.some(
          (key: String) => event.key === key
        )
      ) {
        callbackRef.current(event);
      }
    },
    [keyboardShortcutKeys]
  );

  useEffect(() => {
    const deboucedHandleKeyPress = debounce(handleKeyPress, 25);
    // target is either the provided node or the document
    const targetNode = node ?? document;

    // attach the event listener
    targetNode?.addEventListener("keydown", deboucedHandleKeyPress);

    // remove the event listener
    return () =>
      targetNode &&
      targetNode.removeEventListener("keydown", deboucedHandleKeyPress);
  }, [handleKeyPress, node]);
};

export { useKeysPress, isInputInFocus };
