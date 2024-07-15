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

/* global document, localStorage */

const STORAGE_THEME_KEY = "darkTheme";
const HTML_THEME_DATASET_KEY = "data-color-scheme";
const HTML = document.documentElement;
const TOGGLE_BUTTON_ID = "themeToggleButton";

const getJsonFromStorage = (key) => JSON.parse(localStorage.getItem(key));
const updateTheme = (isDark) => {
  localStorage.setItem(STORAGE_THEME_KEY, isDark);
  HTML.setAttribute(HTML_THEME_DATASET_KEY, isDark ? "dark" : "light");
};
const initTheme = () => {
  const isDark = getJsonFromStorage(STORAGE_THEME_KEY);
  updateTheme(isDark);
};
const toggleTheme = () => {
  const isDark = getJsonFromStorage(STORAGE_THEME_KEY);
  updateTheme(!isDark);
};
document.addEventListener("DOMContentLoaded", () => {
  const themeButton = document.getElementById(TOGGLE_BUTTON_ID);
  themeButton.addEventListener("click", () => {
    toggleTheme();
  });
});
initTheme();
