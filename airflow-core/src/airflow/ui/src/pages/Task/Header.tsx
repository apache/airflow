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
import { useTranslation } from "react-i18next";
import { FiBookOpen } from "react-icons/fi";

import type { TaskResponse } from "openapi/requests/types.gen";
import { TaskIcon } from "src/assets/TaskIcon";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { HeaderCard } from "src/components/HeaderCard";

export const Header = ({ task }: { readonly task: TaskResponse }) => {
  const { t: translate } = useTranslation();

  return (
    <HeaderCard
      actions={
        task.doc_md === null ? undefined : (
          <DisplayMarkdownButton
            header={translate("task.documentation")}
            icon={<FiBookOpen />}
            mdContent={task.doc_md}
            text={translate("docs.documentation")}
          />
        )
      }
      icon={<TaskIcon />}
      stats={[
        { label: translate("task.operator"), value: task.operator_name },
        { label: translate("task.triggerRule"), value: task.trigger_rule },
      ]}
      title={`${task.task_display_name}${task.is_mapped ? " [ ]" : ""}`}
    />
  );
};
