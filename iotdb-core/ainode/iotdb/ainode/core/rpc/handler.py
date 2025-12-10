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

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.cluster_manager import ClusterManager
from iotdb.ainode.core.manager.inference_manager import InferenceManager
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.manager.training_manager import TrainingManager
from iotdb.ainode.core.model.model_info import ModelCategory, ModelInfo, ModelStates
from iotdb.ainode.core.rpc.status import get_status
from iotdb.ainode.core.training.training_parameters import get_default_training_args
from iotdb.ainode.core.util.gpu_mapping import get_available_devices
from iotdb.thrift.ainode import IAINodeRPCService
from iotdb.thrift.ainode.ttypes import (
    TAIHeartbeatReq,
    TAIHeartbeatResp,
    TDeleteModelReq,
    TForecastReq,
    TForecastResp,
    TInferenceReq,
    TInferenceResp,
    TLoadModelReq,
    TRegisterModelReq,
    TRegisterModelResp,
    TShowAIDevicesResp,
    TShowLoadedModelsReq,
    TShowLoadedModelsResp,
    TShowModelsReq,
    TShowModelsResp,
    TTrainingReq,
    TUnloadModelReq,
)
from iotdb.thrift.common.ttypes import TSStatus

logger = Logger()
AIN_CONFIG = AINodeDescriptor().get_config()


def _ensure_device_id_is_available(device_id_list: list[str]) -> TSStatus:
    """
    Ensure that the device IDs in the provided list are available.
    """
    available_devices = get_available_devices()
    for device_id in device_id_list:
        if device_id not in available_devices:
            return TSStatus(
                code=TSStatusCode.INVALID_URI_ERROR.value,
                message=f"Device ID [{device_id}] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.",
            )
    return TSStatus(code=TSStatusCode.SUCCESS_STATUS.value)


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self, ainode):
        self._ainode = ainode
        self._model_manager = ModelManager()
        self._inference_manager = InferenceManager()
        self._training_manager = TrainingManager()

    def stop(self) -> None:
        logger.info("Stopping the RPC service handler of IoTDB-AINode...")
        self._inference_manager.stop()

    def stopAINode(self) -> TSStatus:
        self._ainode.stop()
        return get_status(TSStatusCode.SUCCESS_STATUS, "AINode stopped successfully.")

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        if not AIN_CONFIG.is_activated():
            return TRegisterModelResp(
                status=get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject model registration because TimechoDB-AINode is unactivated.",
                )
            )
        return self._model_manager.register_model(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject model deletion because TimechoDB-AINode is unactivated.",
            )
        return self._model_manager.delete_model(req)

    def showModels(self, req: TShowModelsReq) -> TShowModelsResp:
        return self._model_manager.show_models(req)

    def loadModel(self, req: TLoadModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject load model because TimechoDB-AINode is unactivated.",
            )
        status = self._ensure_model_is_registered(req.existingModelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return status
        status = _ensure_device_id_is_available(req.deviceIdList)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return status
        return self._inference_manager.load_model(req)

    def unloadModel(self, req: TUnloadModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject unload model because TimechoDB-AINode is unactivated.",
            )
        status = self._ensure_model_is_registered(req.modelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return status
        status = _ensure_device_id_is_available(req.deviceIdList)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return status
        return self._inference_manager.unload_model(req)

    def showLoadedModels(self, req: TShowLoadedModelsReq) -> TShowLoadedModelsResp:
        status = _ensure_device_id_is_available(req.deviceIdList)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return TShowLoadedModelsResp(status=status, deviceLoadedModelsMap={})
        return self._inference_manager.show_loaded_models(req)

    def showAIDevices(self) -> TShowAIDevicesResp:
        return TShowAIDevicesResp(
            status=TSStatus(code=TSStatusCode.SUCCESS_STATUS.value),
            deviceIdList=get_available_devices(),
        )

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        if not AIN_CONFIG.is_activated():
            return TInferenceResp(
                get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject inference because TimechoDB-AINode is unactivated.",
                )
            , [])
        status = self._ensure_model_is_registered(req.modelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return TInferenceResp(status, [])
        return self._inference_manager.inference(req)

    def forecast(self, req: TForecastReq) -> TForecastResp:
        if not AIN_CONFIG.is_activated():
            return TForecastResp(get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject forecast because TimechoDB-AINode is unactivated.",
            ), [])
        status = self._ensure_model_is_registered(req.modelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return TForecastResp(status, [])
        return self._inference_manager.forecast(req)

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject training task creation because TimechoDB-AINode is unactivated.",
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

    def _ensure_model_is_registered(self, model_id: str) -> TSStatus:
        if not self._model_manager.is_model_registered(model_id):
            return TSStatus(
                code=TSStatusCode.MODEL_NOT_FOUND_ERROR.value,
                message=f"Model [{model_id}] is not registered yet. You can use 'SHOW MODELS' to retrieve the available models.",
            )
        return TSStatus(code=TSStatusCode.SUCCESS_STATUS.value)
