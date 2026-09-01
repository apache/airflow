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
import { useDurationFormat } from "src/utils/useDurationFormat";

type Props = {
  readonly duration: number | null | undefined;
};

/**
 * A duration in a list column: rounded for scanning, exact on hover.
 *
 * The rounded form loses up to half a unit ("1h 2m" spans a minute), which is too coarse when the
 * point is comparing two similar runs, so the unrounded value rides along in the title. Columns
 * should use this rather than formatting inline, so the pair stays consistent everywhere.
 */
export const DurationCell = ({ duration }: Props) => {
  const { renderDuration, renderExactDuration } = useDurationFormat();

  return <span title={renderExactDuration(duration)}>{renderDuration(duration)}</span>;
};
