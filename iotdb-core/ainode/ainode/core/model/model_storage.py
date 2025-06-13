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
import shutil
from collections.abc import Callable

import torch
import torch._dynamo
from huggingface_hub import hf_hub_download
from pylru import lrucache

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME
from ainode.core.exception import ModelNotExistError
from ainode.core.log import Logger
from ainode.core.model.model_factory import fetch_model_by_uri
from ainode.core.model.sundial import modeling_sundial
from ainode.core.util.lock import ModelLockPool

logger = Logger()


# TODO: All the path must be check and refactor
class ModelStorage(object):
    def __init__(self):
        self._model_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir()
        )
        if not os.path.exists(self._model_dir):
            try:
                os.makedirs(self._model_dir)
            except PermissionError as e:
                logger.error(e)
                raise e
        self._lock_pool = ModelLockPool()
        self._model_cache = lrucache(
            AINodeDescriptor().get_config().get_ain_model_storage_cache_size()
        )

    def register_model(self, model_id: str, uri: str):
        """
        Args:
            model_id: id of model to register
            uri: network dir path or local dir path of model to register, where model.pt and config.yaml are required,
                e.g. https://huggingface.co/user/modelname/resolve/main/ or /Users/admin/Desktop/model
        Returns:
            configs: TConfigs
            attributes: str
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        # create storage dir if not exist
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        model_storage_path = os.path.join(storage_path, DEFAULT_MODEL_FILE_NAME)
        config_storage_path = os.path.join(storage_path, DEFAULT_CONFIG_FILE_NAME)
        return fetch_model_by_uri(uri, model_storage_path, config_storage_path)

    def load_model(self, model_id: str, acceleration: bool) -> Callable:
        """
        Returns:
            model: a ScriptModule contains model architecture and parameters, which can be deployed cross-platform
        """
        ain_models_dir = os.path.join(self._model_dir, "weights", f"{model_id}")
        # TODO: This ugly reading code must be refactored
        if "sundial" in model_id:
            return modeling_sundial.SundialForPrediction.from_pretrained(ain_models_dir)
        model_path = os.path.join(ain_models_dir, DEFAULT_MODEL_FILE_NAME)
        with self._lock_pool.get_lock(model_id).read_lock():
            if model_path in self._model_cache:
                model = self._model_cache[model_path]
                if (
                    isinstance(model, torch._dynamo.eval_frame.OptimizedModule)
                    or not acceleration
                ):
                    return model
                else:
                    model = torch.compile(model)
                    self._model_cache[model_path] = model
                    return model
            else:
                if not os.path.exists(model_path):
                    raise ModelNotExistError(model_path)
                else:
                    model = torch.jit.load(model_path)
                    if acceleration:
                        try:
                            model = torch.compile(model)
                        except Exception as e:
                            logger.warning(
                                f"acceleration failed, fallback to normal mode: {str(e)}"
                            )
                    self._model_cache[model_path] = model
                    return model

    def delete_model(self, model_id: str) -> None:
        """
        Args:
            model_id: id of model to delete
        Returns:
            None
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        with self._lock_pool.get_lock(model_id).write_lock():
            if os.path.exists(storage_path):
                for file_name in os.listdir(storage_path):
                    self._remove_from_cache(os.path.join(storage_path, file_name))
                shutil.rmtree(storage_path)

    def _remove_from_cache(self, file_path: str) -> None:
        if file_path in self._model_cache:
            del self._model_cache[file_path]

    def get_ckpt_path(self, model_id: str) -> str:
        """
        Get the checkpoint path for a given model ID.

        Args:
            model_id (str): The ID of the model.

        Returns:
            str: The path to the checkpoint file for the model.
        """
        # TODO: unify this ugly shit
        if model_id == "sundial" or model_id == "_sundial":
            res = os.path.join(self._model_dir, "weights", "sundial")
            weights_path = os.path.join(res, "model.safetensors")
            if not os.path.exists(weights_path):
                logger.info(
                    f"Weight not found at {weights_path}, downloading from HuggingFace..."
                )
                repo_id = "thuml/sundial-base-128m"
                try:
                    hf_hub_download(
                        repo_id=repo_id,
                        filename="model.safetensors",
                        local_dir=res,
                    )
                    logger.info(f"Got weight to {weights_path}")
                    hf_hub_download(
                        repo_id=repo_id,
                        filename="config.json",
                        local_dir=res,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to download weight to {weights_path} due to {e}"
                    )
                    raise e
            return res
        return os.path.join(self._model_dir, "weights", f"{model_id}")
