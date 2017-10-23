## Variable Editor
----
This folder contains forms used to edit values in the "Variable" key-value
store.  This data can be edited under the "Admin" admin tab, but sometimes
it is preferable to use a form that can perform checking and provide a nicer
interface.

### Adding a new form

1. Create an html template in `templates/variables` folder
2. Provide an interface for the user to provide input data
3. Submit a post request that adds the data as json.  

An example ajax POST request is provided below:
```js
$("#submit-btn").click(function() {
  form_data = getData()
  if (isValid(form_data)) {
    $.ajax({
      method: "POST",
      url: "backfill",
      data: JSON.stringify(form_data),
      dataType: "json",
      contentType: "application/json",
      success: function(response_data) {
        console.log("success.")
      },
      failure: function(response_data) {
        console.log("post error.")            
      }
    })
  }
  else {
    console.log("input error.")
  }
});
```
