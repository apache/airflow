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
import { useState, useCallback } from "react";

import { Field, VStack, Box, Text } from "@chakra-ui/react";
import { useQuery } from "@tanstack/react-query";
import { Select, type SingleValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";

import { resolveExtraLanguages, supportedLanguages } from "src/i18n/config";

const builtinCodes = new Set<string>(supportedLanguages.map((lang) => lang.code));

// The browser's own name for a plugin language code, falling back to the raw code.
const getPluginLanguageName = (code: string): string => {
  try {
    return new Intl.DisplayNames([code], { type: "language" }).of(code) ?? code;
  } catch {
    return code;
  }
};

const LanguageSelector = () => {
  const { i18n, t: translate } = useTranslation();
  const [selectedLang, setSelectedLang] = useState(i18n.resolvedLanguage ?? i18n.language);

  // Plugin-contributed languages, listed by the server, offered alongside the built-in ones.
  const { data: pluginLanguages = [] } = useQuery({
    queryFn: resolveExtraLanguages,
    queryKey: ["uiPluginLanguages"],
  });

  const options = [
    ...supportedLanguages.map((lang) => ({ label: lang.name, value: lang.code })),
    ...pluginLanguages
      .filter((code) => !builtinCodes.has(code))
      .map((code) => ({ label: getPluginLanguageName(code), value: code })),
  ];

  const handleLanguageChange = useCallback(
    (selectedOption: SingleValue<{ label: string; value: string }>) => {
      if (selectedOption) {
        void i18n.changeLanguage(selectedOption.value).then(() => {
          setSelectedLang(selectedOption.value);
        });
      }
    },
    [i18n],
  );

  const currentLang = options.find((option) => option.value === selectedLang);
  const langDir = i18n.dir(selectedLang);

  return (
    <VStack align="stretch" gap={6}>
      <Field.Root>
        <Select<{ label: string; value: string }>
          chakraStyles={{
            clearIndicator: (provided) => ({
              ...provided,
              color: "fg.muted",
            }),
            control: (provided) => ({
              ...provided,
              colorPalette: "input",
            }),
          }}
          onChange={handleLanguageChange}
          options={options}
          placeholder={translate("selectLanguage")}
          value={currentLang}
        />
      </Field.Root>
      <Box borderRadius="md" boxShadow="sm" display="flex" flexDirection="column" gap={2} p={6}>
        <Text fontSize="lg" fontWeight="bold">
          {currentLang?.label} ({selectedLang})
        </Text>
        <Text>{`${translate("direction")}: ${langDir.toUpperCase()}`}</Text>
      </Box>
    </VStack>
  );
};

export default LanguageSelector;
