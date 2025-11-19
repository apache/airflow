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
import { useCallback } from "react";

import {
  useDagServiceGetDagsUiKey,
  useDagServiceFavoriteDag,
  useDagServiceUnfavoriteDag,
  UseDagServiceGetDagDetailsKeyFn,
} from "openapi/queries";

export const useToggleFavoriteDag = (dagId: string) => {
  const queryClient = useQueryClient();

  const onSuccess = useCallback(async () => {
    // Invalidate the DAGs list query
    await queryClient.invalidateQueries({
      queryKey: [useDagServiceGetDagsUiKey, UseDagServiceGetDagDetailsKeyFn({ dagId }, [{ dagId }])],
    });

    const queryKeys = [
      // Invalidate the specific DAG details query for this DAG and DAGs list query.
      UseDagServiceGetDagDetailsKeyFn({ dagId }, [{ dagId }]),
      [useDagServiceGetDagsUiKey],
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));
  }, [queryClient, dagId]);

  const favoriteMutation = useDagServiceFavoriteDag({
    onSuccess,
  });

  const unfavoriteMutation = useDagServiceUnfavoriteDag({
    onSuccess,
  });

  const toggleFavorite = useCallback(
    (isFavorite: boolean) => {
      const mutation = isFavorite ? unfavoriteMutation : favoriteMutation;

      mutation.mutate({ dagId });
    },
    [dagId, favoriteMutation, unfavoriteMutation],
  );

  return {
    error: favoriteMutation.error ?? unfavoriteMutation.error,
    isLoading: favoriteMutation.isPending || unfavoriteMutation.isPending,
    toggleFavorite,
  };
};
