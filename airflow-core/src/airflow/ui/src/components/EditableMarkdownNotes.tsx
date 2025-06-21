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
import EditableMarkdownArea from "./EditableMarkdownArea";
import { Accordion } from "./ui/Accordion";

const EditableMarkdownNotes = ({
  header,
  mdContent,
  onConfirm,
  setMdContent,
}: {
  readonly header: string;
  readonly mdContent?: string | null;
  readonly onConfirm: () => void;
  readonly setMdContent: (value: string) => void;
}) => (
  <Accordion.Root collapsible defaultValue={["notes"]} ms={4} pe={4}>
    <Accordion.Item key="notes" value="notes">
      <Accordion.ItemTrigger cursor="button">{header}</Accordion.ItemTrigger>
      <Accordion.ItemContent
        maxHeight={200}
        onMouseEnter={(event) => {
          event.currentTarget.style.maxHeight = "500px";
        }}
        onMouseLeave={(event) => {
          event.currentTarget.style.maxHeight = "200px";
        }}
        overflowY="auto"
        p={1}
      >
        <EditableMarkdownArea mdContent={mdContent} onBlur={onConfirm} setMdContent={setMdContent} />
      </Accordion.ItemContent>
    </Accordion.Item>
  </Accordion.Root>
);

export default EditableMarkdownNotes;
