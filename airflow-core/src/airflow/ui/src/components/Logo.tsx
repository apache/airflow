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
import type { ComponentProps } from "react";
import { useState } from "react";

import { AirflowPin } from "src/assets/AirflowPin";
import { useColorMode } from "src/context/colorMode";
import { useConfig } from "src/queries/useConfig";

type LogoProps = ComponentProps<typeof AirflowPin>;

export const Logo = ({ height = "1.5em", width = "1.5em", ...rest }: LogoProps) => {
  const theme = useConfig("theme") as unknown as { icon?: string; icon_dark_mode?: string } | undefined;
  const { colorMode } = useColorMode();
  const darkIcon = theme?.icon_dark_mode ?? undefined;
  const lightIcon = theme?.icon ?? undefined;
  const iconSrc = colorMode === "dark" && darkIcon !== undefined ? darkIcon : lightIcon;
  const hasIconSrc = Boolean(iconSrc);
  const [failedLoadingCustomIcon, setFailedLoadingCustomIcon] = useState({ dark: false, light: false });

  if (hasIconSrc && colorMode && !failedLoadingCustomIcon[colorMode]) {
    return (
      // eslint-disable-next-line jsx-a11y/no-noninteractive-element-interactions
      <img
        alt="Logo"
        onError={() => setFailedLoadingCustomIcon((prev) => ({ ...prev, [colorMode]: true }))}
        src={iconSrc}
        // Chakra allows object as 'height' and 'width' but 'img' tag only allows string or number, so we need to check the type before passing it to 'img'
        style={{
          height: typeof height === "string" || typeof height === "number" ? height : "1.5em",
          width: typeof width === "string" || typeof width === "number" ? width : "1.5em",
        }}
      />
    );
  }

  return <AirflowPin height={height} width={width} {...rest} />;
};
