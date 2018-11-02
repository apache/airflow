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
 * @author Andrew Stahlman
 * @version 0.1
 */

// TODO: Move this script and the RSS icon asset to the dpadmin repo
// and publish them to an S3 bucket so any service at Lyft can
// reference them

var DP_ANNOUNCEMENTS_FEED_ATOM_URL = "https://dataplatformadmin.lyft.net/announcements/airflow.atom";
var DP_ANNOUNCEMENTS_FEED_HTML_URL = "https://dataplatformadmin.lyft.net/announcements/airflow";

var setCss = function(el, attrs) {
    el.setAttribute("style", attrs.join('; '));
};

var getXmlElementValue = function(el, node) {
    return el.getElementsByTagName(node)[0].firstChild.nodeValue;
};

var onLoadRssEntries = function() {
    var xmlDoc = this.responseXML;
    var entries = Array.from(xmlDoc.getElementsByTagName("entry"));
    var latestEntry = entries[0];

    var external_link = document.getElementById("announcementsLink");
    var title_box = document.getElementById("announcementHeader");
    var modal_title = document.getElementById("announcementModalTitle");
    var modal_body = document.getElementById("announcementBody");

    var title = getXmlElementValue(latestEntry, "title");
    var summary = getXmlElementValue(latestEntry, "summary");
    var body = getXmlElementValue(latestEntry, "content");


    external_link.setAttribute('href', DP_ANNOUNCEMENTS_FEED_HTML_URL);
    title_box.innerHTML = title;
    modal_title.innerHTML = summary;
    modal_body.innerHTML = body;
};

var populateRssFeed = function() {
    var oReq = new XMLHttpRequest();
    oReq.addEventListener("load", onLoadRssEntries);
    oReq.open("GET", DP_ANNOUNCEMENTS_FEED_ATOM_URL);
    oReq.send();
};

document.addEventListener("DOMContentLoaded", function(event) {
    populateRssFeed();
});
