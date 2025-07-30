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
  readonly "aria-describedby"?: string;
  readonly isPending: boolean;
  readonly onLogin: (loginBody: LoginBody) => void;
};

export const LoginForm = ({ "aria-describedby": ariaDescribedBy, isPending, onLogin }: LoginFormProps) => {
  const {
    control,
    formState: { errors, isValid },
    handleSubmit,
  } = useForm<LoginBody>({
    defaultValues: {
      password: "",
      username: "",
    },
    mode: "onBlur", // Validate on blur for better UX
  });

  return (
    <form
      aria-describedby={ariaDescribedBy}
      noValidate // We handle validation with react-hook-form
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
              <Field.Label htmlFor="username-input">Username</Field.Label>
              <Input
                {...field}
                aria-describedby={fieldState.error ? "username-error" : undefined}
                aria-invalid={Boolean(fieldState.error)}
                aria-required="true"
                autoCapitalize="none"
                autoComplete="username"
                /* eslint-disable-next-line jsx-a11y/no-autofocus */
                autoFocus
                id="username-input"
                spellCheck="false"
                type="text"
                variant="subtle"
              />
              {fieldState.error ? <Field.ErrorText id="username-error" role="alert">
                  Username is required
                </Field.ErrorText> : null}
            </Field.Root>
          )}
          rules={{
            minLength: {
              message: "Username cannot be empty",
              value: 1,
            },
            required: "Username is required",
          }}
        />

        <Controller
          control={control}
          name="password"
          render={({ field, fieldState }) => (
            <Field.Root invalid={Boolean(fieldState.error)} required>
              <Field.Label htmlFor="password-input">Password</Field.Label>
              <Input
                {...field}
                aria-describedby={fieldState.error ? "password-error" : undefined}
                aria-invalid={Boolean(fieldState.error)}
                aria-required="true"
                autoComplete="current-password"
                id="password-input"
                type="password"
                variant="subtle"
              />
              {fieldState.error ? <Field.ErrorText id="password-error" role="alert">
                  Password is required
                </Field.ErrorText> : null}
            </Field.Root>
          )}
          rules={{
            minLength: {
              message: "Password cannot be empty",
              value: 1,
            },
            required: "Password is required",
          }}
        />

        <Button
          aria-describedby={isPending ? "loading-status" : undefined}
          colorScheme="blue"
          disabled={!isValid || isPending}
          loading={isPending}
          loadingText="Signing in..."
          size="lg"
          type="submit"
        >
          {isPending ? "Signing in..." : "Sign in"}
        </Button>

        {isPending ? <span aria-live="polite" className="sr-only" id="loading-status">
            Signing in, please wait...
          </span> : null}
      </Stack>
    </form>
  );
};
