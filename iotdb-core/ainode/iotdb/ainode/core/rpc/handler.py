1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.constant import TSStatusCode
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.manager.cluster_manager import ClusterManager
1from iotdb.ainode.core.manager.inference_manager import InferenceManager
1from iotdb.ainode.core.manager.model_manager import ModelManager
1from iotdb.ainode.core.manager.training_manager import TrainingManager
1from iotdb.ainode.core.model.model_info import ModelCategory, ModelInfo, ModelStates
1from iotdb.ainode.core.rpc.status import get_status
1from iotdb.ainode.core.training.training_parameters import get_default_training_args
1from iotdb.ainode.core.util.gpu_mapping import get_available_devices
1from iotdb.thrift.ainode import IAINodeRPCService
1from iotdb.thrift.ainode.ttypes import (
1    TAIHeartbeatReq,
1    TAIHeartbeatResp,
1    TDeleteModelReq,
1    TForecastReq,
1    TInferenceReq,
1    TInferenceResp,
1    TLoadModelReq,
1    TRegisterModelReq,
1    TRegisterModelResp,
1    TShowAIDevicesResp,
1    TShowLoadedModelsReq,
1    TShowLoadedModelsResp,
1    TShowModelsReq,
1    TShowModelsResp,
1    TTrainingReq,
1    TUnloadModelReq,
1)
1from iotdb.thrift.common.ttypes import TSStatus
1
1logger = Logger()
1AIN_CONFIG = AINodeDescriptor().get_config()
1
1
1def _ensure_device_id_is_available(device_id_list: list[str]) -> TSStatus:
1    """
1    Ensure that the device IDs in the provided list are available.
1    """
1    available_devices = get_available_devices()
1    for device_id in device_id_list:
1        if device_id not in available_devices:
1            return TSStatus(
1                code=TSStatusCode.INVALID_URI_ERROR.value,
1                message=f"Device ID [{device_id}] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.",
1            )
1    return TSStatus(code=TSStatusCode.SUCCESS_STATUS.value)
1
1
1class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
1    def __init__(self, ainode):
1        self._ainode = ainode
1        self._model_manager = ModelManager()
1        self._inference_manager = InferenceManager()
1        self._training_manager = TrainingManager()
1
1    def stop(self) -> None:
1        logger.info("Stopping the RPC service handler of IoTDB-AINode...")
1        self._inference_manager.shutdown()
1
1    def stopAINode(self) -> TSStatus:
1        self._ainode.stop()
1        return get_status(TSStatusCode.SUCCESS_STATUS, "AINode stopped successfully.")
1
1    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
1        if not AIN_CONFIG.is_activated():
1            logger.warning(
1                "TimechoDB-AINode rejects model registration because it is unactivated."
1            )
1            return TRegisterModelResp(
1                status=get_status(
1                    TSStatusCode.AINODE_INTERNAL_ERROR,
1                    "Reject model registration because AINode is unactivated.",
1                )
1            )
1        return self._model_manager.register_model(req)
1
1    def loadModel(self, req: TLoadModelReq) -> TSStatus:
1        status = self._ensure_model_is_built_in_or_fine_tuned(req.existingModelId)
1        if status.code != TSStatusCode.SUCCESS_STATUS.value:
1            return status
1        status = _ensure_device_id_is_available(req.deviceIdList)
1        if status.code != TSStatusCode.SUCCESS_STATUS.value:
1            return status
1        return self._inference_manager.load_model(req)
1
1    def unloadModel(self, req: TUnloadModelReq) -> TSStatus:
1        status = self._ensure_model_is_built_in_or_fine_tuned(req.modelId)
1        if status.code != TSStatusCode.SUCCESS_STATUS.value:
1            return status
1        status = _ensure_device_id_is_available(req.deviceIdList)
1        if status.code != TSStatusCode.SUCCESS_STATUS.value:
1            return status
1        return self._inference_manager.unload_model(req)
1
1    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
1        if not AIN_CONFIG.is_activated():
1            logger.warning(
1                "TimechoDB-AINode rejects model deletion because it is unactivated."
1            )
1            return get_status(
1                TSStatusCode.AINODE_INTERNAL_ERROR,
1                "Reject model deletion because AINode is unactivated.",
1            )
1        return self._model_manager.delete_model(req)
1
1    def inference(self, req: TInferenceReq) -> TInferenceResp:
1        if not AIN_CONFIG.is_activated():
1            logger.warning(
1                "TimechoDB-AINode rejects inference because it is unactivated."
1            )
1            return TInferenceResp(
1                status=get_status(
1                    TSStatusCode.AINODE_INTERNAL_ERROR,
1                    "Reject inference because AINode is unactivated.",
1                )
1            )
1        return self._inference_manager.inference(req)
1
1    def forecast(self, req: TForecastReq) -> TSStatus:
1        if not AIN_CONFIG.is_activated():
1            logger.warning(
1                "TimechoDB-AINode rejects forecast because it is unactivated."
1            )
1            return get_status(
1                TSStatusCode.AINODE_INTERNAL_ERROR,
1                "Reject forecast because AINode is unactivated.",
1            )
1        return self._inference_manager.forecast(req)
1
1    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
1        return ClusterManager.get_heart_beat(req)
1
1    def showModels(self, req: TShowModelsReq) -> TShowModelsResp:
1        return self._model_manager.show_models(req)
1
1    def showLoadedModels(self, req: TShowLoadedModelsReq) -> TShowLoadedModelsResp:
1        status = _ensure_device_id_is_available(req.deviceIdList)
1        if status.code != TSStatusCode.SUCCESS_STATUS.value:
1            return TShowLoadedModelsResp(status=status, deviceLoadedModelsMap={})
1        return self._inference_manager.show_loaded_models(req)
1
1    def showAIDevices(self) -> TShowAIDevicesResp:
1        return TShowAIDevicesResp(
1            status=TSStatus(code=TSStatusCode.SUCCESS_STATUS.value),
1            deviceIdList=get_available_devices(),
1        )
1
1    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
1        if not AIN_CONFIG.is_activated():
1            logger.warning(
1                "TimechoDB-AINode rejects training task creation because it is unactivated."
1            )
1            return get_status(
1                TSStatusCode.AINODE_INTERNAL_ERROR,
1                "Reject training task creation because AINode is unactivated.",
1            )
1        args = get_default_training_args()
1        try:
1            # Parse the request to set up the training parameters
1            args.model_id = req.modelId
1            args.model_type = self._model_manager.get_built_in_model_type(
1                req.existingModelId
1            )
1            args.ckpt_path = self._model_manager.get_ckpt_path(req.existingModelId)
1            args.dataset_type = req.dbType
1            args.data_schema_list = req.targetDataSchema
1            # Parse hyperparameters
1            args.init_from_map(req.parameters)
1            # Initialize GPU configuration
1            args.init_gpu_config()
1            self._model_manager.register_built_in_model(
1                ModelInfo(
1                    model_id=args.model_id,
1                    model_type=args.model_type.value,
1                    category=ModelCategory.FINE_TUNED,
1                    state=ModelStates.TRAINING,
1                )
1            )
1            return self._training_manager.create_training_task(args)
1        except Exception as e:
1            logger.error(f"Failed to parse training configuration: {e}")
1            return get_status(TSStatusCode.INVALID_TRAINING_CONFIG, str(e))
1
1    def _ensure_model_is_built_in_or_fine_tuned(self, model_id: str) -> TSStatus:
1        if not self._model_manager.is_built_in_or_fine_tuned(model_id):
1            return TSStatus(
1                code=TSStatusCode.MODEL_NOT_FOUND_ERROR.value,
1                message=f"Model [{model_id}] is not a built-in or fine-tuned model. You can use 'SHOW MODELS' to retrieve the available models.",
1            )
1        return TSStatus(code=TSStatusCode.SUCCESS_STATUS.value)
1