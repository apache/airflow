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
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";

const FAVORITES_KEY = "favoriteDags";

const getFavoriteDags = (): string[] => {
  const favorites = localStorage.getItem(FAVORITES_KEY);
  return favorites ? JSON.parse(favorites) : [];
};

const setFavoriteDags = (favorites: string[]) => {
  localStorage.setItem(FAVORITES_KEY, JSON.stringify(favorites));
};

export const fetchFavoriteDags = () => {
  return useQuery<string[]>({
    queryKey: [FAVORITES_KEY],
    queryFn: () => Promise.resolve(getFavoriteDags()),
    initialData: [],
  });
};

export const updateFavoriteDags = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ dagId, isFavorite }: { dagId: string; isFavorite: boolean }) => {
      const current = getFavoriteDags();
      const updated = isFavorite
        ? [...new Set([...current, dagId])]
        : current.filter((id) => id !== dagId);
      setFavoriteDags(updated);
      return Promise.resolve(updated);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["favoriteDags"] });
    },
  });
};