

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

//---------------------------------------
// Function for keeping tab focus
// after page reload, uses cookies
//---------------------------------------
$(function()
{
    $('a[data-toggle="tab"]').on('shown.bs.tab', function () {
        //save the latest tab; use cookies if you like 'em better:
        localStorage.setItem('lastTab', $(this).attr('href'));
    });

    //go to the latest tab, if it exists:
    var lastTab = localStorage.getItem('lastTab');
    if (lastTab) {
        $('a[href="' + lastTab + '"]').tab('show');
    }
    else
    {
    // Set the first tab if cookie do not exist
        $('a[data-toggle="tab"]').first().tab('show');
    }

//---------------------------------------
// Function for keeping accordion focus
// after page reload, uses cookies
//---------------------------------------

    $('.panel-collapse').on('shown.bs.collapse', function () {
        //save the latest accordion; use cookies if you like 'em better:
        localStorage.setItem('lastAccordion', $(this).attr('id'));
    });


    $('.panel-collapse').on('hidden.bs.collapse', function () {
        //remove the latest accordion; use cookies if you like 'em better:
        localStorage.removeItem('lastAccordion');
    });


    //go to the latest accordion, if it exists:
    var lastAccordion = localStorage.getItem('lastAccordion');
    if (lastAccordion) {
        if (!($('#' + lastAccordion).hasClass('collapse in')))
        {
            $('#' + lastAccordion).collapse('show');
        }
    }

});
