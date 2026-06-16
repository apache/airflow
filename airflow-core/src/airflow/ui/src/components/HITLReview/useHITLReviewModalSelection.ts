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

export const useHITLReviewModalSelection = ({ hitlDetails }: { readonly hitlDetails: Array<HITLDetail> }) => {
  const [selectedHITLDetailKey, setSelectedHITLDetailKey] = useState<string | undefined>(undefined);

  const selectedIndex = (() => {
    if (hitlDetails.length === 0) {
      return -1;
    }

    const selectedIndexFromKey = hitlDetails.findIndex(
      (hitlDetail) => hitlDetail.task_instance.id === selectedHITLDetailKey,
    );

    return selectedIndexFromKey === -1 ? 0 : selectedIndexFromKey;
  })();
  const isSelected = selectedIndex !== -1;

  const hasNext = isSelected && selectedIndex < hitlDetails.length - 1;

  const onSelect = (hitl?: HITLDetail) => setSelectedHITLDetailKey(hitl?.task_instance.id);

  const onNext = () => {
    if (hasNext) {
      onSelect(hitlDetails[selectedIndex + 1]);
    }
  };

  return {
    onNext,
    onSelect,
    selectedDetail: isSelected ? hitlDetails[selectedIndex] : undefined,
  };
};
