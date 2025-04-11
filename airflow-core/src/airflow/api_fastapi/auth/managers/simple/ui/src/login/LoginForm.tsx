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
import { Button, Field, Input, Stack } from "@chakra-ui/react";
import React from "react";
import { Controller, useForm } from "react-hook-form";

import type { LoginBody } from "./Login";

type LoginFormProps = {
  readonly isPending: boolean;
  readonly onLogin: (loginBody: LoginBody) => void;
};

export const LoginForm = ({ isPending, onLogin }: LoginFormProps) => {
  const {
    control,
    formState: { isValid },
    handleSubmit,
  } = useForm<LoginBody>({
    defaultValues: {
      password: "",
      username: "",
    },
  });

  return (
    <form
      onSubmit={(event: React.SyntheticEvent) => {
        event.preventDefault();
        void handleSubmit(onLogin)();
      }}
    >
      <Stack gap={4}>
        <Controller
          control={control}
          name="username"
          render={({ field, fieldState }) => (
            <Field.Root invalid={Boolean(fieldState.error)} required>
              <Field.Label>Username</Field.Label>
              <Input {...field} />
            </Field.Root>
          )}
          rules={{ required: true }}
        />

        <Controller
          control={control}
          name="password"
          render={({ field, fieldState }) => (
            <Field.Root invalid={Boolean(fieldState.error)} required>
              <Field.Label>Password</Field.Label>
              <Input {...field} type="password" />
            </Field.Root>
          )}
          rules={{ required: true }}
        />

        <Button colorPalette="blue" disabled={!isValid || isPending} type="submit">
          Sign in
        </Button>
      </Stack>
    </form>
  );
};
