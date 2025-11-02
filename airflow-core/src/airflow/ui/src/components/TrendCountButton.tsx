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
import { HStack, Badge, Text, Skeleton } from "@chakra-ui/react";
import { Link, type To } from "react-router-dom";

import { TrendCountChart, type ChartEvent } from "./TrendCountChart";

type Props = {
  readonly colorPalette: string;
  readonly count: number;
  readonly endDate: string;
  readonly events: Array<ChartEvent>;
  readonly isLoading?: boolean;
  readonly label: string;
  readonly route: To;
  readonly startDate: string;
};

export const TrendCountButton = ({
  colorPalette,
  count,
  endDate,
  events,
  isLoading,
  label,
  route,
  startDate,
}: Props) =>
  isLoading ? (
    <Skeleton borderRadius={4} height="45px" width="350px" />
  ) : (
    <Link to={route}>
      <HStack
        _hover={{ bg: "bg.subtle" }}
        borderColor="border.emphasized"
        borderRadius={4}
        borderWidth={1}
        p={3}
        transition="background-color 0.2s"
        width="350px"
      >
        <Badge borderRadius="md" colorPalette={colorPalette} variant="solid">
          {count}
        </Badge>
        <Text fontSize="sm" fontWeight="bold">
          {label}
        </Text>
        <TrendCountChart endDate={endDate} events={events} startDate={startDate} />
      </HStack>
    </Link>
  );
