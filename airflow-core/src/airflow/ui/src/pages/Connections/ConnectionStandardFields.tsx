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
import { Field, Stack, Textarea, Input } from "@chakra-ui/react";
import { useState } from "react";
import { type Control, Controller } from "react-hook-form";
import { FiEye, FiEyeOff } from "react-icons/fi";

import type { StandardFieldSpec } from "src/queries/useConnectionTypeMeta";

import type { ConnectionBody } from "./Connections";

type StandardFieldsProps = {
  readonly control: Control<ConnectionBody, unknown>;
  readonly standardFields: StandardFieldSpec;
};

const StandardFields = ({ control, standardFields }: StandardFieldsProps) => {
  const [showPassword, setShowPassword] = useState(false);

  return (
    <Stack pb={3} pl={3} pr={3}>
      {Object.entries(standardFields).map(([key, fields]) => {
        if (Boolean(fields.hidden)) {
          return undefined;
        }

        return (
          <Controller
            control={control}
            key={key}
            name={key as keyof ConnectionBody}
            render={({ field }) => (
              <Field.Root mt={3} orientation="horizontal">
                <Stack>
                  <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                    {fields.title ?? key}
                  </Field.Label>
                </Stack>
                <Stack css={{ flexBasis: "70%", position: "relative" }}>
                  {key === "description" ? (
                    <Textarea {...field} placeholder={fields.placeholder ?? ""} />
                  ) : (
                    <div style={{ position: "relative", width: "100%" }}>
                      <Input
                        {...field}
                        placeholder={fields.placeholder ?? ""}
                        type={
                          key === "password" && !showPassword
                            ? "password"
                            : key === "port"
                              ? "number"
                              : "text"
                        }
                      />
                      {key === "password" && (
                        <button
                          onClick={() => setShowPassword(!showPassword)}
                          style={{
                            cursor: "pointer",
                            position: "absolute",
                            right: "10px",
                            top: "50%",
                            transform: "translateY(-50%)",
                          }}
                          type="button"
                        >
                          {showPassword ? <FiEye size={15} /> : <FiEyeOff size={15} />}
                        </button>
                      )}
                    </div>
                  )}
                </Stack>
              </Field.Root>
            )}
          />
        );
      })}
    </Stack>
  );
};

export default StandardFields;
