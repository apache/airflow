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

// FIXME: Change to prod before pushing
var DP_ANNOUNCEMENTS_FEED_URL = "https://dataplatformadmin.lyft.net/announcements.atom"

var setCss = function(el, attrs) {
    el.setAttribute("style", attrs.join('; '));
}

var getXmlElementValue = function(el, node) {
    return el.getElementsByTagName(node)[0].firstChild.nodeValue;
}

var onLoadRssEntries = function() {
    var xmlDoc = this.responseXML;
    var entries = Array.from(xmlDoc.getElementsByTagName("entry"));
    var latestEntry = entries[0];

    var rssContainer = document.getElementById("rss-entries");
    var title = getXmlElementValue(latestEntry, "title");

    var entryUrl = (latestEntry.getElementsByTagName("link")[0]).getAttribute("href");
    var titleNode = document.createElement("a");
    titleNode.setAttribute("href", entryUrl);
    titleNode.appendChild(document.createTextNode(title));

    var rssIcon = document.createElement("img");
    rssIcon.setAttribute("src", "../static/rss-logo.png");
    setCss(rssIcon, ["float:right"])

    var rssLink = document.createElement("a");
    rssLink.setAttribute("href", DP_ANNOUNCEMENTS_FEED_URL);
    rssLink.appendChild(rssIcon);

    rssContainer.appendChild(titleNode);
    rssContainer.appendChild(rssLink);
    setCss(rssContainer, ["border:1px",
                          "border-style:solid",
                          "min-width:350px",
                          "max-width:600px",
                          "float:right",
                          "padding:2px",
                          "background-color:linen"]);
}

var populateRssFeed = function() {
    var oReq = new XMLHttpRequest();
    oReq.addEventListener("load", onLoadRssEntries);
    oReq.open("GET", DP_ANNOUNCEMENTS_FEED_URL);
    oReq.send();
}

document.addEventListener("DOMContentLoaded", function(event) {
    populateRssFeed();
});
