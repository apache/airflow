# -*- coding: utf-8 -*-

# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pandas as pd
import os
import tensorflow as tf
from tensorflow.keras import layers


# uncomment and bump up replica_count for distributed training
# strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
# tf.distribute.experimental_set_strategy(strategy)

target = "median_house_value"


def aip_data_to_dataframe(wild_card_path):
    return pd.concat(
        [
            pd.read_csv(fp.numpy().decode())
            for fp in tf.data.Dataset.list_files([wild_card_path])
        ]
    )


def get_features_and_labels(df):
    features = df.drop(target, axis=1)
    return {key: features[key].values for key in features.columns}, df[target].values


def data_prep(wild_card_path):
    return get_features_and_labels(aip_data_to_dataframe(wild_card_path))


train_features, train_labels = data_prep(os.environ["AIP_TRAINING_DATA_URI"])

feature_columns = [
    tf.feature_column.numeric_column(name) for name in train_features.keys()
]

model = tf.keras.Sequential(
    [layers.DenseFeatures(feature_columns), layers.Dense(64), layers.Dense(1)]
)
model.compile(loss="mse", optimizer="adam")

model.fit(
    train_features,
    train_labels,
    epochs=10,
    validation_data=data_prep(os.environ["AIP_VALIDATION_DATA_URI"]),
)
print(model.evaluate(*data_prep(os.environ["AIP_TEST_DATA_URI"])))

# save as Vertex AI Managed model
tf.saved_model.save(model, os.environ["AIP_MODEL_DIR"])
