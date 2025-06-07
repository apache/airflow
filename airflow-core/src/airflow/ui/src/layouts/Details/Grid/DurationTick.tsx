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
import { Text, type TextProps } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

type Props = {
  readonly duration: number;
} & TextProps;

export const DurationTick = ({ duration, ...rest }: Props) => {
  const { t: translate } = useTranslation();

  return (
    <Text color="border.emphasized" fontSize="xs" position="absolute" right={1} whiteSpace="nowrap" {...rest}>
      {translate("duration.seconds", { count: Math.floor(duration) })}
    </Text>
  );
};
