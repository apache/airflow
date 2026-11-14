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
import { Skeleton } from "@chakra-ui/react";

import { HeaderCard, type HeaderCardType } from "src/components/HeaderCard";

/**
 * The line boxes a loaded stat renders into: `fontSize="xs"` at `lineHeight="1"` for the label,
 * one line of `fontSize="sm"` for the value.
 *
 * A placeholder shorter than these leaves the card short by the difference, which is the whole
 * shift this component exists to prevent, so they track the card rather than the look of the bar.
 */
const LABEL_HEIGHT = 12;
const VALUE_HEIGHT = 21;

export type SkeletonStat = {
  /** Roughly the width this stat's label occupies once loaded. */
  readonly labelWidth: number;
  /** Roughly the width this stat's value occupies once loaded. */
  readonly valueWidth: number;
};

type Props = {
  /** One entry per stat the loaded card will show, in the order it will show them. */
  readonly stats: ReadonlyArray<SkeletonStat>;
  readonly titleWidth?: number;
  readonly type: HeaderCardType;
};

/**
 * The header card as it looks before its data arrives.
 *
 * It is a real `HeaderCard` holding skeletons rather than a card-shaped box of its own, so it
 * keeps the padding, the rows and the stat columns of the card it stands in for. The card has no
 * fixed height — it is sized by its content — so matching that content is the only way to hold the
 * same height, and a level that swaps this for its loaded header shifts nothing below it.
 */
export const HeaderCardSkeleton = ({ stats, titleWidth = 160, type }: Props) => (
  <HeaderCard
    icon={<Skeleton borderRadius="full" boxSize={10} />}
    stats={stats.map(({ labelWidth, valueWidth }, index) => ({
      key: `skeleton-stat-${index}`,
      label: <Skeleton height={`${LABEL_HEIGHT}px`} width={`${labelWidth}px`} />,
      value: <Skeleton height={`${VALUE_HEIGHT}px`} width={`${valueWidth}px`} />,
    }))}
    testId="header-card-skeleton"
    title={<Skeleton height="16px" width={`${titleWidth}px`} />}
    type={type}
  />
);
