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
import { Box, Badge } from "@chakra-ui/react";
import { Fragment } from "react";
import { TbLogicOr } from "react-icons/tb";

import { AndGateNode } from "./AndGateNode";
import { AssetNode } from "./AssetNode";
import { OrGateNode } from "./OrGateNode";
import type { ExpressionType, NextRunEvent } from "./types";

export const AssetExpression = ({
  events,
  expression,
}: {
  readonly events?: Array<NextRunEvent>;
  readonly expression: ExpressionType | null;
}) => {
  if (expression === null) {
    return undefined;
  }

  return (
    <>
      {"any" in expression ? (
        <OrGateNode>
          {expression.any?.map((item, index) => (
            // eslint-disable-next-line react/no-array-index-key
            <Fragment key={`any-${index}`}>
              {"asset" in item || "alias" in item ? (
                <AssetNode
                  asset={item}
                  event={events?.find((ev) => "asset" in item && ev.id === item.asset.id)}
                />
              ) : (
                <AssetExpression events={events} expression={item} />
              )}
              {expression.any && index === expression.any.length - 1 ? undefined : (
                <Badge alignItems="center" borderRadius="full" fontSize="sm" px={3} py={1}>
                  <TbLogicOr size={18} />
                  OR
                </Badge>
              )}
            </Fragment>
          ))}
        </OrGateNode>
      ) : undefined}
      {"all" in expression ? (
        <AndGateNode>
          {expression.all?.map((item, index) => (
            // eslint-disable-next-line react/no-array-index-key
            <Box display="inline-block" key={`all-${index}`}>
              {"asset" in item || "alias" in item ? (
                <AssetNode
                  asset={item}
                  event={events?.find((ev) => "asset" in item && ev.id === item.asset.id)}
                />
              ) : (
                <AssetExpression events={events} expression={item} />
              )}
            </Box>
          ))}
        </AndGateNode>
      ) : undefined}
    </>
  );
};
