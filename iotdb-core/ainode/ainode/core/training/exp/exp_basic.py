import os

import numpy as np
import torch
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader

from ainode.core.config import AINodeDescriptor
from ainode.core.ingress.iotdb import DatasetFactory
from ainode.core.log import Logger
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
        self.model_dict = {
            "timerxl": timerxl,
            "sundial": sundial,
        }
        self.config_dict = {
            "timerxl": config_xl,
            "sundial": config_sundial,
        }

        self.rank = rank
        self.gpu_id = args.gpu_ids[rank]
        self.args = args
        self.model = self._build_model()
        (
            self.training_dataset,
            self.training_dataloader,
            self.valid_dataset,
            self.valid_dataloader,
        ) = self._init_data_provider()

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
        if "sundial" in self.args.model_type:
            config = self.config_dict[self.args.model_type].SundialConfig(
                input_token_len=self.args.input_token_len,
                output_token_lens=[self.args.max_output_token_len],
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
            input_len=self.args.input_token_len,
            out_len=self.args.max_output_token_len,
            data_schema_list=self.args.data_schema_list,
            start_split=0,
            end_split=0.9,
        )
        training_dataloader = DataLoader(
            training_dataset,
            batch_size=self.args.batch_size,
            shuffle=True,
            num_workers=self.args.num_workers,
        )
        valid_dataset = dataset(
            model_id=self.args.model_id,
            input_len=self.args.input_token_len,
            out_len=self.args.max_output_token_len,
            data_schema_list=self.args.data_schema_list,
            start_split=0.9,
            end_split=1,
        )
        valid_dataloader = DataLoader(
            valid_dataset,
            batch_size=self.args.batch_size,
            shuffle=False,
            num_workers=self.args.num_workers,
        )
        return training_dataset, training_dataloader, valid_dataset, valid_dataloader

    def train(self):
        pass

    def vali(self):
        pass
