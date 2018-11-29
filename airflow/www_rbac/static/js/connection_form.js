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

/**
 * Created by janomar on 23/07/15.
 */

$(document).ready(function () {
  var config = {
    jdbc: {
      hidden_fields: ['port', 'schema', 'extra'],
      relabeling: {'host': 'Connection URL'},
    },
    google_cloud_platform: {
      hidden_fields: ['host', 'schema', 'login', 'password', 'port', 'extra'],
      relabeling: {},
    },
    cloudant: {
      hidden_fields: ['port', 'extra'],
      relabeling: {
        'host': 'Account',
        'login': 'Username (or API Key)',
        'schema': 'Database'
      }
    },
    docker: {
      hidden_fields: ['port', 'schema'],
      relabeling: {
        'host': 'Registry URL',
        'login': 'Username',
      },
    },
    qubole: {
      hidden_fields: ['login', 'schema', 'port', 'extra'],
      relabeling: {
        'host': 'API Endpoint',
        'password': 'Auth Token',
      }
    },
    ssh: {
      hidden_fields: ['schema'],
      relabeling: {
        'login': 'Username',
      }
    },
  };

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
    if (config[connectionType] != undefined) {
      $.each(config[connectionType].hidden_fields, function (i, field) {
        $("#" + field).parent().parent().addClass('hide')
      });
      $.each(config[connectionType].relabeling, function (k, v) {
        lbl = $("label[for='" + k + "']");
        lbl.attr("orig_text", lbl.text());
        $("label[for='" + k + "']").text(v);
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
