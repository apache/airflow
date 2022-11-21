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

/* global document, CodeMirror, window */

const textArea = document.getElementById('json');
const form = document.getElementById('dag-trigger');
const recentConfigList = document.getElementById('recent_configs');
const minHeight = 300;
const maxHeight = window.innerHeight - 450;
const height = maxHeight > minHeight ? maxHeight : minHeight;

CodeMirror.fromTextArea(textArea, {
  lineNumbers: true,
  mode: {
    name: 'javascript',
    json: true,
  },
  gutters: ['CodeMirror-lint-markers'],
  lint: true,
})
  .setSize(null, height);

function saveConfigJson(e) {
  const { localStorage } = window;
  if (e.preventDefault) e.preventDefault();

  const recentConfigString = localStorage.getItem('recentConfigs') ?? '{}';
  const configsFromStorage = JSON.parse(recentConfigString);
  const jsonString = JSON.stringify(JSON.parse(textArea.value));
  configsFromStorage[jsonString] = Date.now();
  window.localStorage.setItem('recentConfigs', JSON.stringify(configsFromStorage));

  form.submit();
}

function loadRecentConfigs() {
  const { localStorage } = window;
  const recentConfigString = localStorage.getItem('recentConfigs') ?? '{}';
  const configsFromStorage = JSON.parse(recentConfigString);
  const configsToLoad = Object.entries(configsFromStorage)
    .sort(([, a], [, b]) => b - a);

  configsToLoad.forEach((config) => {
    const opt = document.createElement('option');
    [opt.value] = config;
    opt.innerHTML = config[0].replaceAll('"', '&quot;');
    recentConfigList.appendChild(opt);
  });
}

function setRecentConfig(e) {
  document.querySelector('.CodeMirror').CodeMirror.setValue(e.target.value);
}

form.addEventListener('submit', saveConfigJson);
recentConfigList.addEventListener('change', setRecentConfig);
loadRecentConfigs();
