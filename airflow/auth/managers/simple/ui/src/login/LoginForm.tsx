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

import React from "react";
import {Button, Field, Input, Stack} from "@chakra-ui/react";
import {Controller, useForm} from "react-hook-form";
import {LoginBody} from "./Login";

type LoginFormProps = {
    readonly onLogin: (loginBody: LoginBody) => string;
    readonly isPending: boolean;
};

export const LoginForm = ({ onLogin, isPending }: LoginFormProps) => {
    const {
        control, handleSubmit,
        formState: { isValid },
    } = useForm<LoginBody>({
        defaultValues: {
            username: "", password: "",
        },
    });

    return <Stack spacing={4}>
        <form>
            <Controller
                name="username"
                control={control}
                render={({field, fieldState}) => (
                    <Field.Root invalid={Boolean(fieldState.error)} required>
                        <Field.Label>Username</Field.Label>
                        <Input {...field} />
                    </Field.Root>)}
                rules={{required: true}}
            />

            <Controller
                name="password"
                control={control}
                render={({field, fieldState}) => (
                    <Field.Root invalid={Boolean(fieldState.error)} required>
                        <Field.Label>Password</Field.Label>
                        <Input {...field} type="password"/>
                    </Field.Root>)}
                rules={{required: true}}
            />

            <Button disabled={!isValid || isPending} colorPalette="blue" onClick={() => void handleSubmit(onLogin)()} type="submit">
                Sign in
            </Button>
        </form>
    </Stack>
}
