from ainode.core.log import Logger
from ainode.core.util.gpu_mapping import parse_devices

logger = Logger()


class TrainingParameters:
    """
    A class to hold training parameters. Please note that, all parameters should be serializable!
    """

    def __init__(self):
        # Model config
        self.model_type = "sundial"  # The model name to be finetune, options: [sundial], TODO: finetune TimerXL
        self.model_id = "test"  # The model id of the finetune result
        self.ckpt_path = ""  # Checkpoint path, used for finetune

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
        self.devices = ""  # The device ids of GPU for training, set None or "" to occupy all available devices by default
        self.gpu_ids = []  # The list of GPU ids used for training, parsed from devices
        self.world_size = 0  # The number of GPUs used for training

        # Hyper parameters
        self.adaptation = (
            "linear"  # Currently, using linear probing by default, TODO: LoRA
        )
        self.seed = 2021  # help='seed'
        self.task_name = "forecast"  # help='task name, options:[forecast]'
        self.is_training = 1  # help='status'

        # data loader
        # self.checkpoints = (
        #     AINodeDescriptor().get_config().get_ain_ckpts_dir()
        # )  # help='location of model checkpoints'
        # self.data = "ETTh1"  # help='dataset type'
        # self.root_path = './dataset/ETT-small/'  # help='root path of the data file'
        # self.data_path = 'ETTh1.csv'  # help='data file'
        # self.data_list_path = './dataset_list.json'  # help='data list path'
        # self.resume_dir = ''  # help='resume dir'
        # self.scale = True  # help='scale data'

        # forecasting task
        self.seq_len = 672  # help='input sequence length'
        self.input_token_len = 576  # help='input token length'
        self.max_output_token_len = 96  # help='max output token length'

        # test
        self.test_seq_len = 672  # help='test seq len'
        self.test_pred_len = 96  # help='test pred len'
        # self.test_dir = './test'  # help='test dir'
        self.test_with_revin = False  # help='test with revin'
        self.test_n_sample = 500  # help='test n sample'

        # optimization
        self.train_epochs = 10  # help='train epochs'
        self.batch_size = 32  # help='batch size of train input data'
        self.val_batch_size = 32  # help='batch size of val input data'
        self.patience = 3  # help='early stopping patience'
        self.learning_rate = 0.0001  # help='optimizer learning rate'
        self.des = "test"  # help='exp description'
        self.weight_decay = 0  # help='weight decay'
        self.num_warmup_steps = 10000  # help='num warmup steps'
        self.num_training_steps = 100000  # help='num training steps'
        self.iter_per_epoch = 5000  # help='iter per epoch'

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
