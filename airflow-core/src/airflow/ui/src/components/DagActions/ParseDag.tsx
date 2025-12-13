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
import { Button, type ButtonProps } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { AiOutlineFileSync } from "react-icons/ai";

import { useDagParsing } from "src/queries/useDagParsing.ts";

type Props = {
  readonly dagId: string;
  readonly fileToken: string;
} & ButtonProps;

const ParseDag = ({ dagId, fileToken, ...rest }: Props) => {
  const { t: translate } = useTranslation("components");
  const { isPending, mutate } = useDagParsing({ dagId });

  return (
    <Button
      aria-label={translate("reparseDag")}
      loading={isPending}
      onClick={() => mutate({ fileToken })}
      variant="outline"
      {...rest}
    >
      <AiOutlineFileSync height={5} width={5} />
      {translate("reparseDag")}
    </Button>
  );
};

export default ParseDag;
