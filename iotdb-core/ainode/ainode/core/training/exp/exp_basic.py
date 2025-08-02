import os

import numpy as np
import torch
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import TRAINING_LOG_FILE_NAME_PREFIX_TEMPLATE
from ainode.core.ingress.iotdb import DatasetFactory
from ainode.core.log import Logger
from ainode.core.model.model_info import BuiltInModelType
from ainode.core.model.sundial import configuration_sundial as config_sundial
from ainode.core.model.sundial import modeling_sundial as sundial
from ainode.core.model.timerxl import configuration_timer as config_xl
from ainode.core.model.timerxl import modeling_timer as timerxl
from ainode.core.training.training_parameters import TrainingParameters


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
        self.logger = Logger(TRAINING_LOG_FILE_NAME_PREFIX_TEMPLATE.format(self.gpu_id))

        self.args = args
        self.config, self.model = self._build_empty_model()
        self._load_weights_with_adaptation()
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

    def _build_empty_model(self):
        """
        Build the model based on the specified model type in the arguments, with the model weights remain uninitialized.

        Returns:
            config: The configuration object for the model.
            model: The model object initialized with the specified configuration.

        Exceptions:
            ValueError: If the specified model type is not supported.
        """
        # Build a raw model
        # TODO: The ModelManager should take over this process
        if BuiltInModelType.TIMER_XL == self.args.model_type:
            config = self.config_dict[self.args.model_type].TimerConfig(
                input_token_len=self.args.input_token_len,
                output_token_lens=[self.args.output_token_len],
            )
            model = self.model_dict[self.args.model_type].TimerForPrediction(config)
        elif BuiltInModelType.SUNDIAL == self.args.model_type:
            config = self.config_dict[self.args.model_type].SundialConfig(
                input_token_len=self.args.input_token_len,
                output_token_lens=[self.args.output_token_len],
            )
            model = self.model_dict[self.args.model_type].SundialForPrediction(config)
        else:
            raise ValueError(f"Model {self.args.model_type} is not supported.")

        num_params = model.num_parameters()
        self.logger.info(
            f"[Training][GPU-{self.gpu_id}] Model has {num_params:,} parameters."
        )
        # Convert to DDP model
        model = DDP(
            model.cuda(),
            device_ids=[self.gpu_id],
            find_unused_parameters=False,
        )
        return config, model

    def _load_weights_with_adaptation(self):
        # * HF from_pretrained loading ignoring mismatched sizes
        # * - Only loading matched weights
        # * - Unmatched weights remain pre-defined
        # * - DDP is employed by default
        self.model.module = self.model.module.from_pretrained(
            self.args.ckpt_path,
            config=self.config,
            ignore_mismatched_sizes=True,
            torch_dtype=torch.float32,
        ).to(self.gpu_id)
        self.logger.info(
            "[Training][GPU-{}] Finetune model type: {} model id: {} with adaptation: {}".format(
                self.gpu_id,
                self.args.model_type.value,
                self.args.model_id,
                self.args.adaptation,
            )
        )
        # Freeze model weights through specific adaptation method
        if self.args.adaptation == "full":
            # do nothing when full fine-tune
            pass
        elif self.args.adaptation == "linear":
            # * Linear Probing: freezing everything except linear head
            if BuiltInModelType.TIMER_XL == self.args.model_type:
                for name, param in self.model.module.named_parameters():
                    if "lm_head" in name:
                        param.requires_grad = True
                    else:
                        param.requires_grad = False
            elif BuiltInModelType.SUNDIAL == self.args.model_type:
                for name, param in self.model.module.named_parameters():
                    if "flow_loss" in name:
                        # param.data.zero_() # * Zero Initialization for linear probing params
                        param.requires_grad = True
                    else:
                        param.requires_grad = False
                    if self.rank == 0:
                        pass
                    # logger.info(f"{name}: {param.requires_grad}")
        else:
            raise NotImplementedError(
                "[Training][GPU-{}]Adaptation method {} is not implemented.".format(
                    self.gpu_id, self.args.adaptation
                )
            )

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
            window_step=self.args.window_step,
            data_schema_list=self.args.data_schema_list,
            use_rate=0.8,
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
            window_step=self.args.window_step,
            data_schema_list=self.args.data_schema_list,
            use_rate=0.2,
            offset_rate=0.8,
        )
        vali_data_sampler = DistributedSampler(vali_dataset, shuffle=False)
        vali_dataloader = DataLoader(
            vali_dataset,
            batch_size=self.args.vali_batch_size,
            sampler=vali_data_sampler,
            num_workers=self.args.num_workers,
        )
        self.logger.info(
            "[Training][GPU-{}] Init training dataset (len: {}), vali dataset (len: {})".format(
                self.gpu_id, len(training_dataloader), len(vali_dataloader)
            )
        )
        return training_dataset, training_dataloader, vali_dataset, vali_dataloader

    def finetune(self):
        pass

    def validation(self):
        pass
