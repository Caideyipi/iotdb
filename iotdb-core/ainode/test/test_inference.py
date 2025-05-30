# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os
import random
import string

import numpy as np
import pandas as pd
import torch
from iotdb.ainode.config import descriptor
from iotdb.ainode.exception import InferenceModelInternalError
from iotdb.ainode.inference import (
    create_built_in_model,
    inference_with_built_in_model,
    inference_with_registered_model,
    process_data,
)
from sktime.annotation.hmm_learn import GaussianHMM
from sktime.forecasting.arima import ARIMA
from torch import nn


class ExampleModel(nn.Module):
    def __init__(self):
        super(ExampleModel, self).__init__()

    def forward(self, x):
        return x


class ExampleModel2(nn.Module):
    def __init__(self):
        super(ExampleModel2, self).__init__()

    def forward(self, x):
        # assume x is a tensor with shape (1, 3k, 2)
        return x.reshape(1, 3, -1)


model = ExampleModel()
model2 = ExampleModel2()

inference_attributes = {}


def test_process_data_with_bool():
    data = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [1.0, 2.1, 3.3, 4.2, 5.5],
            "c": [True, False, True, True, True],
        }
    )
    time_stamp = pd.DataFrame({"time": [1, 2, 3, 4, 5]})
    type_list = ["INT32", "FLOAT", "BOOLEAN"]
    column_name_list = ["a", "b", "c"]
    tensor_data = torch.tensor(
        [[[1, 1.0, 1], [2, 2.1, 0], [3, 3.3, 1], [4, 4.2, 1], [5, 5.5, 1]]],
        dtype=torch.float64,
    )
    full_data = (data, time_stamp, type_list, column_name_list)
    dataset, dataset_length = process_data(full_data)
    assert dataset.shape == (1, 5, 3)
    assert dataset_length == 5
    assert torch.equal(dataset, tensor_data)


def test_process_data_with_text():
    data = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [1.0, 2.1, 3.3, 4.2, 5.5],
            "c": ["a", "b", "c", "d", "e"],
        }
    )
    time_stamp = pd.DataFrame({"time": [1, 2, 3, 4, 5]})
    type_list = ["INT32", "FLOAT", "TEXT"]
    column_name_list = ["a", "b", "c"]
    tensor_data = torch.tensor(
        [[[1, 1.0, 0], [2, 2.1, 0], [3, 3.3, 0], [4, 4.2, 0], [5, 5.5, 0]]],
        dtype=torch.float64,
    )
    full_data = (data, time_stamp, type_list, column_name_list)
    dataset, dataset_length = process_data(full_data)
    assert dataset.shape == (1, 5, 3)
    assert dataset_length == 5
    assert torch.equal(dataset, tensor_data)


def test_process_data_with_none():
    data = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [1.0, 2.1, 3.3, 4.2, 5.5],
            "c": [None, 1, None, None, None],
        }
    )
    time_stamp = pd.DataFrame({"time": [1, 2, 3, 4, 5]})
    type_list = ["INT32", "FLOAT", "INT32"]
    column_name_list = ["a", "b", "c"]
    tensor_data = torch.tensor(
        [[[1, 1.0, 0], [2, 2.1, 1], [3, 3.3, 0], [4, 4.2, 0], [5, 5.5, 0]]],
        dtype=torch.float64,
    )
    full_data = (data, time_stamp, type_list, column_name_list)
    dataset, dataset_length = process_data(full_data)
    assert dataset.shape == (1, 5, 3)
    assert dataset_length == 5
    assert torch.equal(dataset, tensor_data)


def test_inference_with_multi_window():
    model_id = "".join(random.choice(string.ascii_letters) for x in range(10))
    model_dir = os.path.join(
        os.getcwd(), descriptor.get_config().get_ain_models_dir(), f"{model_id}"
    )
    model_path = os.path.join(model_dir, f"model.pt")
    os.makedirs(model_dir)
    torch.jit.save(torch.jit.script(model), model_path)

    data = pd.DataFrame(
        {
            "a": [random.random() for _ in range(100)],
            "b": [random.random() for _ in range(100)],
        }
    )
    time_stamp = pd.DataFrame({"time": [i for i in range(100)]})
    type_list = ["FLOAT", "FLOAT"]
    column_name_list = ["a", "b"]
    full_data = (data, time_stamp, type_list, column_name_list)
    window_interval = 10
    window_step = 5
    outputs = inference_with_registered_model(
        model_id, full_data, window_interval, window_step, inference_attributes
    )
    assert len(outputs) == 19
    for i in range(19):
        assert outputs[i].shape == (window_interval, 2)


def test_inference_with_single_window():
    model_id = "".join(random.choice(string.ascii_letters) for x in range(10))
    model_dir = os.path.join(
        os.getcwd(), descriptor.get_config().get_ain_models_dir(), f"{model_id}"
    )
    model_path = os.path.join(model_dir, f"model.pt")
    os.makedirs(model_dir)
    torch.jit.save(torch.jit.script(model), model_path)

    data = pd.DataFrame(
        {
            "a": [random.random() for _ in range(100)],
            "b": [random.random() for _ in range(100)],
        }
    )
    time_stamp = pd.DataFrame({"time": [i for i in range(100)]})
    type_list = ["FLOAT", "FLOAT"]
    column_name_list = ["a", "b"]
    full_data = (data, time_stamp, type_list, column_name_list)
    window_interval = 20
    window_step = float("inf")
    outputs = inference_with_registered_model(
        model_id, full_data, window_interval, window_step, inference_attributes
    )
    assert len(outputs) == 1
    assert outputs[0].shape == (window_interval, 2)


def test_inference_with_acceleration():
    model_id = "".join(random.choice(string.ascii_letters) for x in range(10))
    model_dir = os.path.join(
        os.getcwd(), descriptor.get_config().get_ain_models_dir(), f"{model_id}"
    )
    model_path = os.path.join(model_dir, f"model.pt")
    os.makedirs(model_dir)
    torch.jit.save(torch.jit.script(model), model_path)

    data = pd.DataFrame(
        {
            "a": [random.random() for _ in range(100)],
            "b": [random.random() for _ in range(100)],
        }
    )
    time_stamp = pd.DataFrame({"time": [i for i in range(100)]})
    type_list = ["FLOAT", "FLOAT"]
    column_name_list = ["a", "b"]
    full_data = (data, time_stamp, type_list, column_name_list)
    window_interval = 20
    window_step = float("inf")
    outputs = inference_with_registered_model(
        model_id, full_data, window_interval, window_step, {"acceleration": "true"}
    )
    assert len(outputs) == 1
    assert outputs[0].shape == (window_interval, 2)


def test_inference_with_invalid_window_interval():
    model_id = "".join(random.choice(string.ascii_letters) for x in range(10))
    model_dir = os.path.join(
        os.getcwd(), descriptor.get_config().get_ain_models_dir(), f"{model_id}"
    )
    model_path = os.path.join(model_dir, f"model.pt")
    os.makedirs(model_dir)

    torch.jit.save(torch.jit.script(model), model_path)

    data = pd.DataFrame(
        {
            "a": [random.random() for _ in range(100)],
            "b": [random.random() for _ in range(100)],
        }
    )
    time_stamp = pd.DataFrame({"time": [i for i in range(100)]})
    type_list = ["FLOAT", "FLOAT"]
    column_name_list = ["a", "b"]
    full_data = (data, time_stamp, type_list, column_name_list)
    window_interval = 101
    window_step = 5
    try:
        outputs = inference_with_registered_model(
            model_id, full_data, window_interval, window_step, inference_attributes
        )
    except Exception as e:
        assert str(
            e
        ) == "Invalid inference input: window_interval {0}, window_step {1}, dataset_length {2}".format(
            window_interval, window_step, 100
        )


def test_inference_with_none_window_interval():
    model_id = "".join(random.choice(string.ascii_letters) for x in range(10))
    model_dir = os.path.join(
        os.getcwd(), descriptor.get_config().get_ain_models_dir(), f"{model_id}"
    )
    model_path = os.path.join(model_dir, f"model.pt")
    os.makedirs(model_dir)

    torch.jit.save(torch.jit.script(model), model_path)

    data = pd.DataFrame(
        {
            "a": [random.random() for _ in range(100)],
            "b": [random.random() for _ in range(100)],
        }
    )
    time_stamp = pd.DataFrame({"time": [i for i in range(100)]})
    type_list = ["FLOAT", "FLOAT"]
    column_name_list = ["a", "b"]
    full_data = (data, time_stamp, type_list, column_name_list)
    window_interval = None
    window_step = 5
    try:
        outputs = inference_with_registered_model(
            model_id, full_data, window_interval, window_step, inference_attributes
        )
    except Exception as e:
        assert str(
            e
        ) == "Invalid inference input: window_interval {0}, window_step {1}, dataset_length {2}".format(
            window_interval, window_step, 100
        )


def test_inference_with_internal_error():
    model_id = "".join(random.choice(string.ascii_letters) for x in range(10))
    model_dir = os.path.join(
        os.getcwd(), descriptor.get_config().get_ain_models_dir(), f"{model_id}"
    )
    model_path = os.path.join(model_dir, f"model.pt")
    os.makedirs(model_dir)

    torch.jit.save(torch.jit.script(model2), model_path)

    data = pd.DataFrame(
        {
            "a": [random.random() for _ in range(100)],
            "b": [random.random() for _ in range(100)],
        }
    )
    time_stamp = pd.DataFrame({"time": [i for i in range(100)]})
    type_list = ["FLOAT", "FLOAT"]
    column_name_list = ["a", "b"]
    full_data = (data, time_stamp, type_list, column_name_list)
    window_interval = 20
    window_step = 5
    try:
        outputs = inference_with_registered_model(
            model_id, full_data, window_interval, window_step, inference_attributes
        )
    except Exception as e:
        assert isinstance(e, InferenceModelInternalError)


def test_inference_with_built_in_model_forecasting():
    data = [float(i) for i in range(100)]
    data = pd.DataFrame({"a": data})
    time_stamp = pd.DataFrame({"time": [i for i in range(100)]})
    type_list = ["FLOAT"]
    column_name_list = ["a"]
    full_data = (data, time_stamp, type_list, column_name_list)

    outputs = inference_with_built_in_model(
        "_arima", full_data, {"predict_length": "10", "order": "(1,1,1)"}
    )
    assert len(outputs) == 1
    assert outputs[0].shape == (10, 1)

    outputs = inference_with_built_in_model(
        "_exponentialsmoothing", full_data, {"predict_length": "10"}
    )
    assert len(outputs) == 1
    assert outputs[0].shape == (10, 1)

    outputs = inference_with_built_in_model(
        "_naiveforecaster", full_data, {"predict_length": "10"}
    )
    assert len(outputs) == 1
    assert outputs[0].shape == (10, 1)

    outputs = inference_with_built_in_model(
        "_stlforecaster", full_data, {"predict_length": "10"}
    )
    assert len(outputs) == 1
    assert outputs[0].shape == (10, 1)


def test_inference_with_built_in_model_anomaly_detection():
    data = [float(1) for _ in range(90)]
    data.extend([float(10000) for _ in range(10)])
    data = pd.DataFrame({"a": data})
    time_stamp = pd.DataFrame({"time": [i for i in range(100)]})
    type_list = ["FLOAT", "FLOAT"]
    column_name_list = ["a", "b"]
    full_data = (data, time_stamp, type_list, column_name_list)

    outputs = inference_with_built_in_model(
        "_gaussianhmm", full_data, {"n_components": "2", "n_iter": "1000"}
    )
    assert len(outputs) == 1
    assert outputs[0].shape == (100, 1)

    outputs = inference_with_built_in_model("_gmmhmm", full_data, {})
    assert len(outputs) == 1
    assert outputs[0].shape == (100, 1)

    outputs = inference_with_built_in_model("_stray", full_data, {})
    assert len(outputs) == 1
    assert outputs[0].shape == (100, 1)
