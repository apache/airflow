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
import type { FlexibleFormElementProps } from ".";
import { NumberInputField, NumberInputRoot } from "../ui/NumberInput";

export const isFieldNumber = (fieldType: string) => {
  const numberTypes = ["integer", "number"];

  return numberTypes.includes(fieldType);
};

export const FlexibleFormFieldNumber = ({ name, param }: FlexibleFormElementProps) => (
  <NumberInputRoot
    allowMouseWheel
    defaultValue={String(param.value)}
    id={`element_${name}`}
    max={param.schema.maximum ?? undefined}
    min={param.schema.minimum ?? undefined}
    name={`element_${name}`}
    size="sm"
  >
    <NumberInputField />
  </NumberInputRoot>
);
