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
from ainode.core.config import AINodeDescriptor
from ainode.core.constant import TSStatusCode
from ainode.core.log import Logger
from ainode.core.manager.cluster_manager import ClusterManager
from ainode.core.manager.inference_manager import InferenceManager
from ainode.core.manager.model_manager import ModelManager
from ainode.core.manager.training_manager import TrainingManager
from ainode.core.model.model_info import ModelCategory, ModelInfo, ModelStates
from ainode.core.rpc.status import get_status
from ainode.core.training.training_parameters import get_default_training_args
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
    TShowModelsReq,
    TShowModelsResp,
    TTrainingReq,
)
from ainode.thrift.common.ttypes import TSStatus

logger = Logger()
AIN_CONFIG = AINodeDescriptor().get_config()


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self, ainode):
        self._ainode = ainode
        self._model_manager = ModelManager()
        self._inference_manager = InferenceManager()
        self._training_manager = TrainingManager()

    def stopAINode(self) -> TSStatus:
        self._ainode.stop()
        return get_status(TSStatusCode.SUCCESS_STATUS, "AINode stopped successfully.")

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        if not AIN_CONFIG.is_activated():
            logger.warning(
                "TimechoDB-AINode rejects model registration because it is unactivated."
            )
            return TRegisterModelResp(
                status=get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject model registration because AINode is unactivated.",
                )
            )
        return self._model_manager.register_model(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            logger.warning(
                "TimechoDB-AINode rejects model deletion because it is unactivated."
            )
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject model deletion because AINode is unactivated.",
            )
        return self._model_manager.delete_model(req)

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        if not AIN_CONFIG.is_activated():
            logger.warning(
                "TimechoDB-AINode rejects inference because it is unactivated."
            )
            return TInferenceResp(
                status=get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject inference because AINode is unactivated.",
                )
            )
        return self._inference_manager.inference(req)

    def forecast(self, req: TForecastReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            logger.warning(
                "TimechoDB-AINode rejects forecast because it is unactivated."
            )
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject forecast because AINode is unactivated.",
            )
        return self._inference_manager.forecast(req)

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def showModels(self, req: TShowModelsReq) -> TShowModelsResp:
        return self._model_manager.show_models(req)

    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            logger.warning(
                "TimechoDB-AINode rejects training task creation because it is unactivated."
            )
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject training task creation because AINode is unactivated.",
            )
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
