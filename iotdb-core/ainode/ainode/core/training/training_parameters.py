from ainode.core.log import Logger
from ainode.core.model.model_info import BuiltInModelType
from ainode.core.util.gpu_mapping import parse_devices

logger = Logger()


class TrainingParameters:
    """
    A class to hold training parameters. Please note that, all parameters should be serializable!
    """

    def __init__(self):
        # Model config
        self.model_type = (
            BuiltInModelType.SUNDIAL
        )  # The model name to be finetune, options: [sundial], TODO: finetune TimerXL
        self.model_id = "test"  # The model id of the finetune result
        self.ckpt_path = ""  # Checkpoint path, used for finetune

        self.seq_len = 2880  # The number of time series data points for each input training data window
        self.input_token_len = (
            16  # The number of time series data points for each token
        )
        self.output_token_len = (
            720  # The number of time series data points for each output
        )

        # Dataset and Dataloader
        self.dataset_type = (
            ""  # The dataset type for training, options: [iotdb.table, iotdb.tree]
        )
        self.data_schema_list = (
            []
        )  # The list of data schemas for the dataset, used to initialization
        self.num_workers = 1  # The number of data loader workers, TODO: solve concurrent bug before using it
        self.aggregation_interval = (
            "1s"  # The aggregation interval of dataset, TODO: support this function
        )

        # GPU
        self.ddp = True  # Enabling DistributedDataParallel (DDP) by default
        self.devices = ""  # The device ids of GPU for training, set None or "" to occupy all available devices by default TODO: we have problems when parsing this
        self.gpu_ids = []  # The list of GPU ids used for training, parsed from devices
        self.world_size = 0  # The number of GPUs used for training

        # Hyper parameters
        self.adaptation = (
            "linear"  # Currently, using linear probing by default
            # "full"
            # "linear"
            # "lora" TODO: support LoRA
        )
        self.seed = 2021  # help='seed'

        # training
        self.train_epochs = 10  # help='train epochs'
        self.training_batch_size = 64  # help='batch size of train input data'
        self.num_warmup_steps = 10000  # help='num warmup steps'
        self.num_training_steps = 100000  # help='num training steps'
        self.iter_per_epoch = 5000  # help='iter per epoch'
        self.revin = True  # help='test with revin'
        self.learning_rate = 0.00001  # help='optimizer learning rate'
        self.weight_decay = 0.1  # help='weight decay'
        self.only_preserve_best = (
            True  # Only preserve the best ckpt during training, by default is True
        )
        self.patience = 3  # The number of epochs to wait for improvement before early stopping, TODO: Enable this function

        # validation
        self.vali_batch_size = 40
        self.vali_pred_len = (
            self.output_token_len
        )  # we keep the vali pred len same as output len
        self.vali_n_samples = 10

    def init_from_map(self, config_map: dict):
        if config_map is None:
            return
        for key, value in config_map.items():
            if hasattr(self, key):
                attr_type = type(getattr(self, key))
                if attr_type == bool:
                    value = value.lower() in ("true", "1")
                elif attr_type == int:
                    value = int(value)
                elif attr_type == float:
                    value = float(value)

                setattr(self, key, value)
            else:
                logger.warning(
                    f"{key} is not a valid config key of TrainingParameters."
                )

    def init_gpu_config(self):
        """
        Initialize the GPU configuration for DDP training.
        """
        self.devices = parse_devices(self.devices)
        self.gpu_ids = [int(gpu) for gpu in self.devices.split(",")]
        self.world_size = len(self.gpu_ids)


def get_default_training_args() -> TrainingParameters:
    args = TrainingParameters()
    return args
