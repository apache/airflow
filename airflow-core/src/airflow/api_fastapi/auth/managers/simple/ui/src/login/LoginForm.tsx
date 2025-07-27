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
  readonly "aria-describedby"?: string;
};

export const LoginForm = ({ isPending, onLogin, "aria-describedby": ariaDescribedBy }: LoginFormProps) => {
  const {
    control,
    formState: { isValid, errors },
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
      onSubmit={(event: React.SyntheticEvent) => {
        event.preventDefault();
        void handleSubmit(onLogin)();
      }}
      aria-describedby={ariaDescribedBy}
      noValidate // We handle validation with react-hook-form
    >
      <Stack gap={4}>
        <Controller
          control={control}
          name="username"
          render={({ field, fieldState }) => (
            <Field.Root 
              invalid={Boolean(fieldState.error)} 
              required
            >
              <Field.Label htmlFor="username-input">
                Username
              </Field.Label>
              <Input 
                {...field}
                id="username-input"
                type="text"
                variant="subtle"
                autoComplete="username"
                autoCapitalize="none"
                spellCheck="false"
                aria-required="true"
                aria-invalid={Boolean(fieldState.error)}
                aria-describedby={fieldState.error ? "username-error" : undefined}
                /* eslint-disable-next-line jsx-a11y/no-autofocus */
                autoFocus
              />
              {fieldState.error && (
                <Field.ErrorText id="username-error" role="alert">
                  Username is required
                </Field.ErrorText>
              )}
            </Field.Root>
          )}
          rules={{ 
            required: "Username is required",
            minLength: {
              value: 1,
              message: "Username cannot be empty"
            }
          }}
        />

        <Controller
          control={control}
          name="password"
          render={({ field, fieldState }) => (
            <Field.Root 
              invalid={Boolean(fieldState.error)} 
              required
            >
              <Field.Label htmlFor="password-input">
                Password
              </Field.Label>
              <Input 
                {...field}
                id="password-input"
                type="password" 
                variant="subtle"
                autoComplete="current-password"
                aria-required="true"
                aria-invalid={Boolean(fieldState.error)}
                aria-describedby={fieldState.error ? "password-error" : undefined}
              />
              {fieldState.error && (
                <Field.ErrorText id="password-error" role="alert">
                  Password is required
                </Field.ErrorText>
              )}
            </Field.Root>
          )}
          rules={{ 
            required: "Password is required",
            minLength: {
              value: 1,
              message: "Password cannot be empty"
            }
          }}
        />

        <Button 
          disabled={!isValid || isPending} 
          type="submit"
          size="lg"
          colorScheme="blue"
          aria-describedby={isPending ? "loading-status" : undefined}
          loadingText="Signing in..."
          loading={isPending}
        >
          {isPending ? "Signing in..." : "Sign in"}
        </Button>
        
        {isPending && (
          <span id="loading-status" className="sr-only" aria-live="polite">
            Signing in, please wait...
          </span>
        )}
      </Stack>
    </form>
  );
};
