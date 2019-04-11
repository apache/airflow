/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import {defaultFormatWithTZ, moment} from './datetime-utils';

function displayTime() {
  let utcTime = moment().utc().format(defaultFormatWithTZ);
  $('#clock')
    .attr("data-original-title", function() {
      return hostName
    })
    .html(utcTime);

  setTimeout(displayTime, 1000);
}

var el = document.createElement("span");

export function escapeHtml(text) {
  el.textContent = text;
  return el.innerHTML;
}

window.escapeHtml = escapeHtml;

function postAsForm(url, parameters) {
  var form = $("<form></form>");

  form.attr("method", "POST");
  form.attr("action", url);

  $.each(parameters || {}, function(key, value) {
    var field = $('<input></input>');

    field.attr("type", "hidden");
    field.attr("name", key);
    field.attr("value", value);

    form.append(field);
  });

  var field = $('<input></input>');

  field.attr("type", "hidden");
  field.attr("name", "csrf_token");
  field.attr("value", csrfToken);

  form.append(field);

  // The form needs to be a part of the document in order for us to be able
  // to submit it.
  $(document.body).append(form);
  form.submit();
}

window.postAsForm = postAsForm;

$(document).ready(function () {
  displayTime();
  $('span').tooltip();
  $.ajaxSetup({
    beforeSend: function(xhr, settings) {
      if (!/^(GET|HEAD|OPTIONS|TRACE)$/i.test(settings.type) && !this.crossDomain) {
        xhr.setRequestHeader("X-CSRFToken", csrfToken);
      }
    }
  });
});
