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
import { ButtonGroup, IconButton } from "@chakra-ui/react";
import { MdCompress, MdExpand } from "react-icons/md";

type Props = {
  readonly collapseLabel: string;
  readonly expandLabel: string;
  readonly isCollapseDisabled?: boolean;
  readonly isExpandDisabled?: boolean;
  readonly onCollapse: () => void;
  readonly onExpand: () => void;
};

export const ExpandCollapseButtons = ({
  collapseLabel,
  expandLabel,
  isCollapseDisabled,
  isExpandDisabled,
  onCollapse,
  onExpand,
  ...rest
}: Props) => (
  <ButtonGroup attached size="sm" variant="surface" {...rest}>
    <IconButton
      aria-label={expandLabel}
      disabled={isExpandDisabled}
      onClick={onExpand}
      size="sm"
      title={expandLabel}
    >
      <MdExpand />
    </IconButton>
    <IconButton
      aria-label={collapseLabel}
      disabled={isCollapseDisabled}
      onClick={onCollapse}
      size="sm"
      title={collapseLabel}
    >
      <MdCompress />
    </IconButton>
  </ButtonGroup>
);
