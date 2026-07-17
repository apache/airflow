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
import { Button, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuUserRoundPen } from "react-icons/lu";
import { Link } from "react-router-dom";

import type { HITLDetail } from "openapi/requests/types.gen";
import { HITLReviewModal } from "src/components/HITLReview/HITLReviewModal.tsx";
import { StateBadge } from "src/components/StateBadge";
import { Tooltip } from "src/components/ui";

type Props = {
  readonly pendingActions: Array<HITLDetail>;
};

export const NeedsReviewBadge = ({ pendingActions }: Props) => {
  const { t: translate } = useTranslation("hitl");
  const { onClose, onOpen, open } = useDisclosure();

  if (pendingActions.length === 0) {
    return undefined;
  }

  return (
    <>
      <Tooltip content={translate("requiredActionCount", { count: pendingActions.length })}>
        <Button data-testid="needs-review-badge" onClick={onOpen} variant="plain">
          <StateBadge colorPalette="awaiting_input" fontSize="md" variant="solid">
            <LuUserRoundPen />
            {pendingActions.length}
          </StateBadge>
        </Button>
      </Tooltip>
      <HITLReviewModal
        headerAction={
          <Button asChild size="sm" variant="outline">
            <Link onClick={onClose} to="/required_actions?response_received=false">
              {translate("review.viewAll")}
            </Link>
          </Button>
        }
        onClose={onClose}
        open={open}
        pendingHitl={{ data: pendingActions, isError: false, isLoading: false }}
      />
    </>
  );
};
