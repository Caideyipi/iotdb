import os
import threading
import time
from typing import Dict

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.model.model_info import ModelStates
from iotdb.ainode.core.rpc.client import ClientManager
from iotdb.ainode.core.rpc.status import get_status
from iotdb.ainode.core.training.exp.exp_forecast_finetune import ExpForecastFinetune
from iotdb.ainode.core.training.training_parameters import TrainingParameters
from iotdb.ainode.core.util.lock import ReadWriteLock
from iotdb.thrift.common.ttypes import TSStatus

logger = Logger()

DEFAULT_MASTER_ADDR = "localhost"
DEFAULT_MASTER_PORT = "64209"  # TODO: generate different ports for each process


# TODO: Starting from this interface, the log files should be distributed according to diff GPU process.
def _start_training(
    rank: int, args: TrainingParameters, status_dict: Dict[int, TSStatus]
):
    gpu_id = args.gpu_ids[rank]
    try:
        torch.cuda.set_device(gpu_id)
        master_addr = os.environ.get("MASTER_ADDR", DEFAULT_MASTER_ADDR)
        master_port = os.environ.get("MASTER_PORT", DEFAULT_MASTER_PORT)
        dist.init_process_group(
            backend="nccl",  # Use NCCL by default
            init_method=f"tcp://{master_addr}:{master_port}",
            world_size=args.world_size,
            rank=rank,
        )
        logger.info(
            f"[Training][GPU-{gpu_id}] Start training model_id: {args.model_id}, model_type: {args.model_type.value} based on ckpt: {args.ckpt_path}."
        )
        exp = ExpForecastFinetune(rank, args)
        exp.finetune()
        logger.info(
            f"[Training][GPU-{gpu_id}] The training task of model_id: {args.model_id}, model_type: {args.model_type.value} is finished."
        )
        status_dict[rank] = get_status(
            TSStatusCode.SUCCESS_STATUS,
            f"[Training][GPU-{gpu_id}] Training finished successfully.",
        )
    except Exception as e:
        logger.error(
            f"[Training][GPU-{gpu_id}] The training task of model_id: {args.model_id}, model_type: {args.model_type.value} is failed, because {e}."
        )
        status_dict[rank] = get_status(TSStatusCode.TRAINING_INTERNAL_ERROR, str(e))
    finally:
        dist.destroy_process_group()


TRAINING_BIG_LOCK = ReadWriteLock()  # TODO: definitely required optimize


def _init_training(args: TrainingParameters, model_manager: ModelManager):
    with TRAINING_BIG_LOCK.write_lock():
        # Setup training environment variables for DDP
        os.environ["MASTER_ADDR"] = DEFAULT_MASTER_ADDR
        os.environ["MASTER_PORT"] = DEFAULT_MASTER_PORT
        status_dict = mp.Manager().dict()
        mp.spawn(
            _start_training,
            args=(args, status_dict),
            nprocs=args.world_size,
            join=False,
        )
        while len(status_dict) < args.world_size:
            # Wait for all processes to finish
            time.sleep(5)
        all_successful = all(
            result.code == TSStatusCode.SUCCESS_STATUS.get_status_code()
            for result in status_dict.values()
        )
        if all_successful:
            ClientManager().borrow_config_node_client().update_model_info(
                args.model_id,
                3,  # TODO: use enum
                "finished",
                [AINodeDescriptor().get_config().get_ainode_id()],
                args.seq_len,
                args.output_token_len,
            )
            model_manager.update_model_state(args.model_id, ModelStates.ACTIVE)
        else:
            # TODO: handle and delete this model on IoTDB
            ClientManager().borrow_config_node_client().update_model_info(
                args.model_id,
                0,
                "Please check the training logs for more details.",
                [AINodeDescriptor().get_config().get_ainode_id()],
                args.seq_len,
                args.output_token_len,
            )
            model_manager.update_model_state(args.model_id, ModelStates.FAILED)


class TrainingManager:
    """
    A manager class for handling training tasks in a parallel manner using the ParallelTaskExecutor.
    """

    def __init__(self):
        self._model_manager = ModelManager()
        # self.executor = ParallelTaskExecutor()
        # self.executor.start()

    def create_training_task(self, args: TrainingParameters):
        """
        Create a training task with the given parameters and submit it to the executor.
        Args:
            args (TrainingParameters): The training parameters.
        """
        try:
            # TODO: Wrap with ParallelTaskExecutor if necessary
            training_thread = threading.Thread(
                target=_init_training, args=(args, self._model_manager)
            )
            training_thread.daemon = True
            training_thread.start()
            return get_status(
                TSStatusCode.SUCCESS_STATUS, "Training task created successfully."
            )
        except Exception as e:
            logger.error(e)
            return get_status(TSStatusCode.TRAINING_INTERNAL_ERROR, str(e))
