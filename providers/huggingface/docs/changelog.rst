 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

 .. NOTE TO CONTRIBUTORS:
    Please, only add notes to the Changelog just below the "Changelog" header when there
    are some breaking changes and you want to add an explanation to the users on how they
    are supposed to deal with them. The changelog is updated and maintained semi-automatically
    by release manager.

``apache-airflow-providers-huggingface``


Changelog
---------

.. note::
    ``StreamDatasetOperator`` now passes ``embed_external_files=False`` to ``push_to_hub`` when the source
    is a Hub repository. Rows that reference a media file by path are published with that path, not the
    file's bytes. With Datasets 5.0 and 5.1 this includes a Hub image folder, which those versions stream
    as file references; 5.2.0 streams it as bytes. Set ``push_kwargs={"embed_external_files": True}`` to
    use the Datasets default as before.

0.1.0
.....

Initial version of the provider.
