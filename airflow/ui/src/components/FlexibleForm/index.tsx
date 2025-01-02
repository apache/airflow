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
import { Stack, StackSeparator } from "@chakra-ui/react";

import type { DagParamsSpec, ParamSpec } from "src/queries/useDagParams";

import { FlexibleFormRow } from "./FlexibleFormRow";

type FlexibleFormProps = {
  readonly params: DagParamsSpec;
};

export type FlexibleFormElementProps = {
  readonly key: string;
  readonly name: string;
  readonly param: ParamSpec;
};

const FlexibleForm = ({ params }: FlexibleFormProps) => (
  // TODO: Support multiple sections - at the moment all is rendered flat
  // TODO: Add a note that the form is not "working" until onBlur not implemented
  //       ...or add a note as altert when the form is "used"
  <Stack separator={<StackSeparator />}>
    {Object.entries(params).map(([name, param]) => (
      <FlexibleFormRow key={name} name={name} param={param} />
    ))}
  </Stack>
);

export default FlexibleForm;
