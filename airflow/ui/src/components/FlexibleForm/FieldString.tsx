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
import { Input } from "@chakra-ui/react";

import type { FlexibleFormElementProps } from ".";

export const FieldString = ({ name, param }: FlexibleFormElementProps) => (
  <>
    <Input
      defaultValue={String(param.value ?? "")}
      id={`element_${name}`}
      list={param.schema.examples ? `list_${name}` : undefined}
      maxLength={param.schema.maxLength ?? undefined}
      minLength={param.schema.minLength ?? undefined}
      name={`element_${name}`}
      placeholder={param.schema.examples ? "Start typing to see options." : undefined}
      size="sm"
    />
    {param.schema.examples ? (
      <datalist id={`list_${name}`}>
        {param.schema.examples.map((example) => (
          <option key={example} value={example}>
            {example}
          </option>
        ))}
      </datalist>
    ) : undefined}
  </>
);
