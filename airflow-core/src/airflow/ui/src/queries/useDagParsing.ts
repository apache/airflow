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
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import {
  useDagParsingServiceReparseDagFile,
  UseDagServiceGetDagDetailsKeyFn,
  UseDagSourceServiceGetDagSourceKeyFn,
} from "openapi/queries";
import { toaster } from "src/components/ui";

export const useDagParsing = ({ dagId }: { readonly dagId: string }) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("dag");

  const onError = () => {
    toaster.create({
      description: translate("parse.toaster.error.description"),
      title: translate("parse.toaster.error.title"),
      type: "error",
    });
  };

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: UseDagServiceGetDagDetailsKeyFn({ dagId }),
    });

    await queryClient.invalidateQueries({
      queryKey: UseDagSourceServiceGetDagSourceKeyFn({ dagId }),
    });

    toaster.create({
      description: translate("parse.toaster.success.description"),
      title: translate("parse.toaster.success.title"),
      type: "success",
    });
  };

  const { isPending, mutate } = useDagParsingServiceReparseDagFile({
    onError,
    onSuccess,
  });

  return {
    isPending,
    mutate,
  };
};
