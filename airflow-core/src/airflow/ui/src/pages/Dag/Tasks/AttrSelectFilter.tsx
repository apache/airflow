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

import {Select} from "src/components/ui";
import {type CollectionItem, createListCollection} from "@chakra-ui/react";
import {useTranslation} from "react-i18next";

type Props = {
  readonly handleOperatorSelect: (value: CollectionItem) => void;
  readonly operatorNames: Array<string>;
  readonly selectedOperator: string;
};


export const AttrSelectFilter = ({
  handleOperatorSelect,
  operatorNames,
  selectedOperator,
}: Props) => {
  const { t: translate } = useTranslation();

  const totalOperatorList = [ translate("allOperators"), ...(operatorNames ?? [])]
  const operatorCollection = createListCollection({items: totalOperatorList})

  return (
  <Select.Root
    collection={operatorCollection}
    maxW="200px"
    onValueChange={handleOperatorSelect}
    value={[selectedOperator]}
  >
    <Select.Trigger colorPalette="blue" minW="max-content" >
      <Select.ValueText placeholder={translate("selectOperator")} width="auto">
        {() => selectedOperator}
      </Select.ValueText>
    </Select.Trigger>
    <Select.Content>
      {operatorCollection.items.map((option) => (
        <Select.Item item={option} key={option}>
          {option}
        </Select.Item>
      ))}
    </Select.Content>
  </Select.Root>

    );
};
