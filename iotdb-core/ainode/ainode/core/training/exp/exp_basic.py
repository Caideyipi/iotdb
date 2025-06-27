import os
import shutil

import numpy as np
import torch
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler

from ainode.core.config import AINodeDescriptor
from ainode.core.ingress.iotdb import DatasetFactory
from ainode.core.log import Logger
from ainode.core.model.model_info import BuiltInModelType
from ainode.core.model.sundial import configuration_sundial as config_sundial
from ainode.core.model.sundial import modeling_sundial as sundial
from ainode.core.model.timerxl import configuration_timer as config_xl
from ainode.core.model.timerxl import modeling_timer as timerxl
from ainode.core.training.training_parameters import TrainingParameters

logger = Logger()


class ExpBasic(object):
    def __init__(self, rank: int, args: TrainingParameters):
        self._model_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir()
        )
        self._built_in_model_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_builtin_models_dir()
        )
        self.model_dict = {
            BuiltInModelType.TIMER_XL: timerxl,
            BuiltInModelType.SUNDIAL: sundial,
        }
        self.config_dict = {
            BuiltInModelType.TIMER_XL: config_xl,
            BuiltInModelType.SUNDIAL: config_sundial,
        }

        self.rank = rank
        self.gpu_id = args.gpu_ids[rank]
        self.args = args
        self.model = self._build_model()
        (
            self.training_dataset,
            self.training_dataloader,
            self.vali_dataset,
            self.vali_dataloader,
        ) = self._init_data_provider()
        self.mse_loss_list = []
        self.mae_loss_list = []

        fix_seed = args.seed
        torch.manual_seed(fix_seed)
        np.random.seed(fix_seed)

    def _build_model(self):
        """
        Build the model based on the specified model type in the arguments.

        Exceptions:
            ValueError: If the specified model type is not supported.
        """
        # Build a raw model
        # TODO: The ModelManager should take over this process
        if BuiltInModelType.TIMER_XL == self.args.model_type:
            config = self.config_dict[self.args.model_type].TimerConfig(
                input_token_len=self.args.input_token_len
            )
            model = self.model_dict[self.args.model_type].TimerForPrediction(config)
        elif BuiltInModelType.SUNDIAL == self.args.model_type:
            config = self.config_dict[self.args.model_type].SundialConfig(
                output_token_lens=[self.args.output_token_len],
            )
            model = self.model_dict[self.args.model_type].SundialForPrediction(config)
        else:
            raise ValueError(f"Model {self.args.model_type} is not supported.")

        num_params = model.num_parameters()
        logger.info(
            f"[Training][GPU-{self.gpu_id}] Model has {num_params:,} parameters."
        )
        # Convert to DDP model
        model = DDP(
            model.cuda(),
            device_ids=[self.gpu_id],
            find_unused_parameters=False,
        )
        return model

    def _init_data_provider(self):
        """
        Initialize the training and validation datasets based on the provided schema list.
        """
        dataset = DatasetFactory().get_dataset(self.args.dataset_type)
        training_dataset = dataset(
            model_id=self.args.model_id,
            seq_len=self.args.seq_len,
            input_token_len=self.args.input_token_len,
            output_token_len=self.args.output_token_len,
            data_schema_list=self.args.data_schema_list,
            use_rate=0.75,
            offset_rate=0.0,
        )
        training_data_sampler = DistributedSampler(training_dataset, shuffle=True)
        training_dataloader = DataLoader(
            training_dataset,
            batch_size=self.args.training_batch_size,
            sampler=training_data_sampler,
            num_workers=self.args.num_workers,
        )
        vali_dataset = dataset(
            model_id=self.args.model_id,
            seq_len=self.args.seq_len,
            input_token_len=self.args.input_token_len,
            output_token_len=self.args.output_token_len,
            data_schema_list=self.args.data_schema_list,
            use_rate=0.25,
            offset_rate=0.75,
        )
        vali_data_sampler = DistributedSampler(vali_dataset, shuffle=False)
        vali_dataloader = DataLoader(
            vali_dataset,
            batch_size=self.args.vali_batch_size,
            sampler=vali_data_sampler,
            num_workers=self.args.num_workers,
        )
        logger.info(
            "[Training][GPU-{}] Init training dataset (len: {}), vali dataset (len: {})".format(
                self.gpu_id, len(training_dataloader), len(vali_dataloader)
            )
        )
        return training_dataset, training_dataloader, vali_dataset, vali_dataloader

    def finetune(self):
        pass

    def validation(self):
        pass
