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
import time
from typing import Dict

import torch
import torch.nn as nn

from iotdb.ainode.config import descriptor
from iotdb.ainode.constant import (
    DEFAULT_CONFIG_FILE_NAME,
    DEFAULT_MODEL_FILE_NAME,
    ModelInputName,
)
from iotdb.ainode.exception import ModelNotExistError
from iotdb.ainode.storage import model_storage
from iotdb.thrift.ainode.ttypes import TConfigs


class ExampleModel(nn.Module):
    def __init__(self):
        super(ExampleModel, self).__init__()
        self.layer = nn.Identity()

    def forward(self, input_dict: Dict):
        # x: [Batch, Input length, Channel]
        x = input_dict[ModelInputName.DATA_X.value]
        return self.layer(x)


class ExampleModel2(nn.Module):
    def __init__(self):
        super(ExampleModel2, self).__init__()

    def forward(self, x):
        return x


model = ExampleModel()
model2 = ExampleModel2()
model_config = {"input_length": 1, "input_vars": 1, "id": time.time()}


def test_save_model():
    trial_id = "tid_0"
    model_id = "mid_test_model_save"
    model_storage.save_model(model, model_config, model_id=model_id, trial_id=trial_id)
    assert os.path.exists(
        os.path.join(
            descriptor.get_config().get_ain_models_dir(),
            f"{model_id}",
            f"{trial_id}.pt",
        )
    )


def test_load_model():
    trial_id = "tid_0"
    model_id = "mid_test_model_load"

    model_file_path = os.path.join(f"{model_id}", f"{trial_id}.pt")

    model_storage.save_model(model, model_config, model_id=model_id, trial_id=trial_id)
    _, model_config_loaded = model_storage.load_model(model_file_path)
    assert model_config == model_config_loaded


def test_load_not_exist_model():
    trial_id = "dummy_trial"
    model_id = "dummy_model"
    try:
        model_storage.load_model(os.path.join(f"{model_id}", f"{trial_id}.pt"))
    except Exception as e:
        assert (
            e.message
            == ModelNotExistError(
                os.path.join(
                    os.getcwd(),
                    descriptor.get_config().get_ain_models_dir(),
                    model_id,
                    f"{trial_id}.pt",
                )
            ).message
        )


storage_path = "./data/ainode/models"


def test_register_and_delete_model_from_local():
    model_id = "dlinear"
    uri = "'./test/resource/dlinear'"
    configs, attributes = model_storage.register_model(model_id, uri)
    assert os.path.exists(os.path.join(storage_path, f"{model_id}", "model.pt"))
    assert os.path.exists(os.path.join(storage_path, f"{model_id}", "config.yaml"))
    assert configs.__class__ == TConfigs
    assert attributes.__class__ == str
    assert configs.input_shape == [96, 2]
    assert configs.output_shape == [96, 2]
    assert configs.input_type == [4, 2]
    assert configs.output_type == [5, 2]
    assert attributes == "{'model_type': 'dlinear', 'kernel_size': '25'}"
    model_storage.delete_model(model_id=model_id)
    assert not os.path.exists(os.path.join(storage_path, f"{model_id}"))


def test_register_and_delete_model_from_local_without_type():
    model_id = "dlinear"
    uri = "'./test/resource/dlinear_without_type'"
    configs, attributes = model_storage.register_model(model_id, uri)
    assert os.path.exists(os.path.join(storage_path, f"{model_id}", "model.pt"))
    assert os.path.exists(os.path.join(storage_path, f"{model_id}", "config.yaml"))
    assert configs.__class__ == TConfigs
    assert attributes.__class__ == str
    assert configs.input_shape == [96, 2]
    assert configs.output_shape == [96, 2]
    assert configs.input_type == [4, 4]
    assert configs.output_type == [4, 4]
    assert attributes == "{'model_type': 'dlinear', 'kernel_size': '25'}"
    model_storage.delete_model(model_id=model_id)
    assert not os.path.exists(os.path.join(storage_path, f"{model_id}"))


def test_register_and_delete_model_from_local_without_attributes():
    model_id = "dlinear"
    uri = "'./test/resource/dlinear_without_attributes'"
    configs, attributes = model_storage.register_model(model_id, uri)
    assert os.path.exists(os.path.join(storage_path, f"{model_id}", "model.pt"))
    assert os.path.exists(os.path.join(storage_path, f"{model_id}", "config.yaml"))
    assert configs.__class__ == TConfigs
    assert attributes.__class__ == str
    assert configs.input_shape == [96, 2]
    assert configs.output_shape == [96, 2]
    assert configs.input_type == [4, 4]
    assert configs.output_type == [4, 4]
    assert attributes == ""
    model_storage.delete_model(model_id=model_id)
    assert not os.path.exists(os.path.join(storage_path, f"{model_id}"))


def test_register_and_delete_model_from_local_with_file_prefix():
    model_id = "dlinear"
    uri = "'file://./test/resource/dlinear'"
    configs, attributes = model_storage.register_model(model_id, uri)
    assert os.path.exists(os.path.join(storage_path, f"{model_id}", "model.pt"))
    assert os.path.exists(os.path.join(storage_path, f"{model_id}", "config.yaml"))
    assert configs.__class__ == TConfigs
    assert attributes.__class__ == str
    assert configs.input_shape == [96, 2]
    assert configs.output_shape == [96, 2]
    assert configs.input_type == [4, 2]
    assert configs.output_type == [5, 2]
    assert attributes == "{'model_type': 'dlinear', 'kernel_size': '25'}"
    model_storage.delete_model(model_id=model_id)
    assert not os.path.exists(os.path.join(storage_path, f"{model_id}"))


def test_register_model_from_wrong_local():
    model_id = "dlinear_dummy"
    uri = "'./test/resource/dlinear_dummy'"
    try:
        model_storage.register_model(model_id, uri)
    except Exception as e:
        assert (
            e.message
            == f"Invalid uri: {uri[1:-1]}, there are no {DEFAULT_MODEL_FILE_NAME} or {DEFAULT_CONFIG_FILE_NAME} under this uri."
        )


def test_load_model_from_id():
    model_id = "".join(random.choice(string.ascii_letters) for x in range(10))
    model_dir = os.path.join(
        os.getcwd(), descriptor.get_config().get_ain_models_dir(), f"{model_id}"
    )
    model_path = os.path.join(model_dir, f"model.pt")
    os.makedirs(model_dir)
    torch.jit.save(torch.jit.script(model2), model_path)
    model_loaded = model_storage.load_model_from_id(model_id)
    input_tensor = torch.randn(5, 5, 5)
    assert isinstance(model_loaded, torch.jit.ScriptModule)
    assert torch.equal(model_loaded(input_tensor), model2(input_tensor))


def test_load_not_exist_model_from_id():
    model_id = "dummy_model"
    model_dir = os.path.join(
        os.getcwd(), descriptor.get_config().get_ain_models_dir(), f"{model_id}"
    )
    model_path = os.path.join(model_dir, f"model.pt")
    try:
        model_storage.load_model_from_id(model_id)
    except Exception as e:
        assert e.message == ModelNotExistError(os.path.join(model_path)).message
