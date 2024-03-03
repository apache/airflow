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

import React from "react";
import { Box, Heading, Flex, Spinner, Button } from "@chakra-ui/react";

import { useDataset } from "src/api";
import { ClipboardButton } from "src/components/Clipboard";
import InfoTooltip from "src/components/InfoTooltip";
import Events from "./DatasetEvents";

interface Props {
  uri: string;
  onBack: () => void;
}

const DatasetDetails = ({ uri, onBack }: Props) => {
  const { data: dataset, isLoading } = useDataset({ uri });
  return (
    <Box mt={[6, 3]}>
      <Button onClick={onBack}>See all datasets</Button>
      {isLoading && <Spinner display="block" />}
      <Box>
        <Heading my={2} fontWeight="normal" size="lg">
          Dataset: {uri}
          <ClipboardButton value={uri} iconOnly ml={2} />
        </Heading>
      </Box>
      <Flex alignItems="center">
        <Heading size="md" mt={3} mb={2} fontWeight="normal">
          History
        </Heading>
        <InfoTooltip
          label="Whenever a DAG has updated this dataset."
          size={18}
        />
      </Flex>
      {dataset && dataset.id && <Events datasetId={dataset.id} />}
    </Box>
  );
};

export default DatasetDetails;
