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

function setRecentConfig(e) {
  let { value } = e.target;
  try {
    const json = JSON.parse(value);
    value = JSON.stringify(json, null, 2);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('config is not valid JSON format');
  }
  document.querySelector('.CodeMirror').CodeMirror.setValue(value);
}

recentConfigList.addEventListener('change', setRecentConfig);
