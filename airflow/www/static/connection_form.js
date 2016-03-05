/**
 * Created by janomar on 23/07/15.
 */

  $(document).ready(function() {
      var config = {
        jdbc: {
            hidden_fields: ['port', 'schema', 'extra'],
            relabeling: {'host': 'Connection URL'},
        },
        google_cloud_platform: {
            hidden_fields: ['host', 'schema', 'login', 'password', 'port', 'extra'],
            relabeling: {},
        }

      }
      function connTypeChange(connectionType) {
        $("div.form_group").removeClass("hide");
        $.each($("[id^='extra__']"), function() {
            $(this).parent().parent().addClass('hide')
        });
        // Somehow the previous command doesn't honor __
        $("#extra").parent().parent().removeClass('hide')
        $.each($("[id^='extra__"+connectionType+"']"), function() {
            $(this).parent().parent().removeClass('hide')
        });
        $("label[orig_text]").each(function(){
            $(this).text($(this).attr("orig_text"));
        });
        if (config[connectionType] != undefined){
          $.each(config[connectionType].hidden_fields, function(i, field){
            $("#" + field).parent().parent().addClass('hide')
          });
          $.each(config[connectionType].relabeling, function(k, v){
            lbl = $("label[for='" + k + "']")
            lbl.attr("orig_text", lbl.text());
            $("label[for='" + k + "']").text(v);
          });
        }
      }
      var connectionType=$("#conn_type").val();
      $("#conn_type").on('change', function(e) {
        connectionType = $("#conn_type").val();
        connTypeChange(connectionType);
      });
      connTypeChange(connectionType);
});
