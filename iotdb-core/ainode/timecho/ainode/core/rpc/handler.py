from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_constants import ModelCategory, ModelStates
from iotdb.ainode.core.model.model_info import ModelInfo
from iotdb.ainode.core.rpc.handler import AINodeRPCServiceHandler
from iotdb.ainode.core.rpc.status import get_status
from iotdb.thrift.ainode import IAINodeRPCService
from iotdb.thrift.ainode.ttypes import (
    TDeleteModelReq,
    TForecastReq,
    TForecastResp,
    TInferenceReq,
    TInferenceResp,
    TLoadModelReq,
    TRegisterModelReq,
    TRegisterModelResp,
    TTuningReq,
    TUnloadModelReq,
)
from iotdb.thrift.common.ttypes import TSStatus
from timecho.ainode.core.manager.tuning_manager import TuningManager
from timecho.ainode.core.tuning.training_parameters import get_default_training_args

logger = Logger()
AIN_CONFIG = AINodeDescriptor().get_config()


class TimechoAINodeRPCServiceHandler(AINodeRPCServiceHandler, IAINodeRPCService.Iface):
    def __init__(self, ainode):
        super().__init__(ainode)
        self._tuning_manager = TuningManager()

    # ==================== Model Management ====================

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        if not AIN_CONFIG.is_activated():
            return TRegisterModelResp(
                status=get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject model registration because TimechoDB-AINode is unactivated.",
                )
            )
        return super().registerModel(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject model deletion because TimechoDB-AINode is unactivated.",
            )
        return super().deleteModel(req)

    def loadModel(self, req: TLoadModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject load model because TimechoDB-AINode is unactivated.",
            )
        return super().loadModel(req)

    def unloadModel(self, req: TUnloadModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject unload model because TimechoDB-AINode is unactivated.",
            )
        return super().unloadModel(req)

    # ==================== Inference ====================

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        if not AIN_CONFIG.is_activated():
            return TInferenceResp(
                get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject inference because TimechoDB-AINode is unactivated.",
                ),
                [],
            )
        return super().inference(req)

    def forecast(self, req: TForecastReq) -> TForecastResp:
        if not AIN_CONFIG.is_activated():
            return TForecastResp(
                get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject forecast because TimechoDB-AINode is unactivated.",
                ),
                [],
            )
        return super().forecast(req)

    # ==================== Tuning ====================

    def createTuningTask(self, req: TTuningReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject tuning task creation because TimechoDB-AINode is unactivated.",
            )
        args = get_default_training_args()
        try:
            # Parse the request to set up the tuning parameters
            args.model_id = req.modelId
            args.model_type = self._model_manager.get_model_info(
                req.existingModelId
            ).model_type
            args.ckpt_path = self._model_manager.get_ckpt_path(req.existingModelId)
            args.dataset_type = req.dbType
            args.data_schema_list = req.targetDataSchema
            # Parse hyperparameters
            args.init_from_map(req.parameters)
            # Initialize GPU configuration
            args.init_gpu_config()
            self._model_manager.register_fine_tuned_model(
                ModelInfo(
                    model_id=args.model_id,
                    model_type=args.model_type,
                    category=ModelCategory.FINE_TUNED,
                    state=ModelStates.TRAINING,
                )
            )
            return self._tuning_manager.create_tuning_task(args)
        except Exception as e:
            logger.error(f"Failed to parse tuning configuration: {e}")
            return get_status(TSStatusCode.INVALID_TRAINING_CONFIG, str(e))
