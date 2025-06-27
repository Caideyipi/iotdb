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
from ainode.core.constant import TSStatusCode
from ainode.core.log import Logger
from ainode.core.manager.cluster_manager import ClusterManager
from ainode.core.manager.inference_manager import InferenceManager
from ainode.core.manager.model_manager import ModelManager
from ainode.core.manager.training_manager import TrainingManager
from ainode.core.model.model_info import ModelCategory, ModelInfo, ModelStates
from ainode.core.training.training_parameters import get_default_training_args
from ainode.core.util.status import get_status
from ainode.thrift.ainode import IAINodeRPCService
from ainode.thrift.ainode.ttypes import (
    TAIHeartbeatReq,
    TAIHeartbeatResp,
    TDeleteModelReq,
    TForecastReq,
    TInferenceReq,
    TInferenceResp,
    TRegisterModelReq,
    TRegisterModelResp,
    TShowModelsResp,
    TTrainingReq,
)
from ainode.thrift.common.ttypes import TSStatus

logger = Logger()


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self):
        self._model_manager = ModelManager()
        self._inference_manager = InferenceManager(model_manager=self._model_manager)
        self._training_manager = TrainingManager(model_manager=self._model_manager)

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        return self._model_manager.register_model(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        return self._model_manager.delete_model(req)

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        return self._inference_manager.inference(req)

    def forecast(self, req: TForecastReq) -> TSStatus:
        return self._inference_manager.forecast(req)

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def showModels(self) -> TShowModelsResp:
        return self._model_manager.show_models()

    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
        args = get_default_training_args()
        try:
            # Parse the request to set up the training parameters
            args.model_id = req.modelId
            args.model_type = self._model_manager.get_built_in_model_type(
                req.existingModelId
            )
            args.ckpt_path = self._model_manager.get_ckpt_path(req.existingModelId)
            args.dataset_type = req.dbType
            args.data_schema_list = req.targetDataSchema
            # Parse hyperparameters
            args.init_from_map(req.parameters)
            # Initialize GPU configuration
            args.init_gpu_config()
            self._model_manager.register_built_in_model(
                ModelInfo(
                    model_id=args.model_id,
                    model_type=args.model_type.value,
                    category=ModelCategory.FINE_TUNED,
                    state=ModelStates.TRAINING,
                )
            )
            return self._training_manager.create_training_task(args)
        except Exception as e:
            logger.error(f"Failed to parse training configuration: {e}")
            return get_status(TSStatusCode.INVALID_TRAINING_CONFIG, str(e))
