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
/**
 * Created by janomar on 23/07/15.
 */

function decode(str) {
  return new DOMParser().parseFromString(str, "text/html").documentElement.textContent
}

$(document).ready(function () {

  function connTypeChange(connectionType) {
    $(".hide").removeClass("hide");
    $.each($("[id^='extra__']"), function () {
      $(this).parent().parent().addClass('hide')
    });
    $.each($("[id^='extra__" + connectionType + "']"), function () {
      $(this).parent().parent().removeClass('hide')
    });
    $("label[orig_text]").each(function () {
      $(this).text($(this).attr("orig_text"));
    });
    $(".form-control").each(function(){$(this).attr('placeholder', '')});
    let config = JSON.parse(decode($("#field_behaviours").text()))
    if (config[connectionType] != undefined) {
      $.each(config[connectionType].hidden_fields, function (i, field) {
        $("#" + field).parent().parent().addClass('hide')
      });
      $.each(config[connectionType].relabeling, function (k, v) {
        lbl = $("label[for='" + k + "']");
        lbl.attr("orig_text", lbl.text());
        $("label[for='" + k + "']").text(v);
      });
      $.each(config[connectionType].placeholders, function(k, v){
        $("#" + k).attr('placeholder', v);
      });
    }
  }

  var connectionType = $("#conn_type").val();
  $("#conn_type").on('change', function (e) {
    connectionType = $("#conn_type").val();
    connTypeChange(connectionType);
  });
  connTypeChange(connectionType);
});
