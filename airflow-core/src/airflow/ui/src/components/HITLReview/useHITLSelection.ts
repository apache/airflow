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
import { useState } from "react";

import type { HITLDetail } from "openapi/requests/types.gen.ts";

export const useHITLSelection = ({ hitlDetails }: { readonly hitlDetails: Array<HITLDetail> }) => {
  const [selectedKey, setSelectedKey] = useState<string | undefined>(undefined);
  const selectedIndexFromKey = hitlDetails.findIndex(
    (hitlDetail) => hitlDetail.task_instance.id === selectedKey,
  );
  const selectedIndex =
    hitlDetails.length > 0 ? (selectedIndexFromKey === -1 ? 0 : selectedIndexFromKey) : -1;
  const selectedDetail = selectedIndex === -1 ? undefined : hitlDetails[selectedIndex];
  const hasNext = selectedIndex !== -1 && selectedIndex < hitlDetails.length - 1;
  const hasPrevious = selectedIndex > 0;

  const selectHitl = (hitl?: HITLDetail) => setSelectedKey(hitl?.task_instance.id);

  const onNext = () => {
    if (hasNext) {
      selectHitl(hitlDetails[selectedIndex + 1]);
    }
  };

  const onPrevious = () => {
    if (hasPrevious) {
      selectHitl(hitlDetails[selectedIndex - 1]);
    }
  };

  return {
    hasNext,
    hasPrevious,
    onNext,
    onPrevious,
    onSelect: selectHitl,
    selectedDetail,
    selectedKey: selectedDetail?.task_instance.id,
  };
};
