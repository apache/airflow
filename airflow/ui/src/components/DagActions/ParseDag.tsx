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
import { AiOutlineFileSync } from "react-icons/ai";

import { Button } from "src/components/ui";
import { useDagParsing } from "src/queries/useDagParsing.ts";

type Props = {
  readonly dagId: string;
  readonly fileToken: string;
};

const ParseDag = ({ dagId, fileToken }: Props) => {
  const { isPending, mutate } = useDagParsing({ dagId });

  return (
    <Button
      aria-label="Reparse Dag"
      loading={isPending}
      onClick={() => mutate({ fileToken })}
      variant="outline"
    >
      <AiOutlineFileSync height={5} width={5} />
      Reparse Dag
    </Button>
  );
};

export default ParseDag;
