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
import type { RefObject } from "react";

export const setRefStyle = <T extends HTMLElement | null>(
  ref: RefObject<T> | null | undefined,
  property: string,
  value: string,
): void => {
  if (ref?.current && ref.current instanceof HTMLElement) {
    ref.current.style.setProperty(property, value);
  }
};

export const setRefOpacity = <T extends HTMLElement | null>(
  ref: RefObject<T> | null | undefined,
  opacity: "0" | "1",
): void => {
  setRefStyle(ref, "opacity", opacity);
};

export const setRefTransform = <T extends HTMLElement | null>(
  ref: RefObject<T> | null | undefined,
  transform: string,
): void => {
  setRefStyle(ref, "transform", transform);
};

export const setRefTransformAndOpacity = <T extends HTMLElement | null>(
  ref: RefObject<T> | null | undefined,
  transform: string | undefined,
  opacity: "0" | "1",
): void => {
  if (transform !== undefined) {
    setRefTransform(ref, transform);
  }
  setRefOpacity(ref, opacity);
};
