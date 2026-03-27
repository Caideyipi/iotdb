import random

import numpy as np
import torch
from torch import nn

from iotdb.ainode.core.model.dual_weaver_sundial.configuration_dual_weaver_sundial import (
    DualWeaverSundialConfig,
)
from iotdb.ainode.core.model.sundial.modeling_sundial import (
    SundialForPrediction,
    SundialPreTrainedModel,
)


class WeaverCNN(nn.Module):
    def __init__(self, configs):
        super().__init__()
        self.configs = configs
        if configs.input_token_len == configs.output_token_len:
            self.conv1 = nn.Sequential(
                nn.Conv1d(
                    configs.input_channel,
                    configs.output_channel,
                    5,
                    1,
                    2,
                    1,
                    padding_mode="replicate",
                ),
                nn.LayerNorm([configs.output_channel, configs.seq_len]),
                nn.SiLU(),
                nn.Dropout1d(p=0.1),
            )
            self.conv2 = nn.Sequential(
                nn.Conv1d(
                    configs.output_channel,
                    configs.input_channel,
                    5,
                    1,
                    2,
                    1,
                    padding_mode="replicate",
                ),
                nn.LayerNorm([configs.input_channel, configs.seq_len]),
                nn.SiLU(),
                nn.Dropout1d(p=0.1),
            )
        else:
            self.conv1 = nn.Conv1d(
                configs.input_channel,
                configs.output_channel,
                5,
                1,
                2,
                1,
                padding_mode="replicate",
            )
            self.layernorm_1_x = nn.LayerNorm([configs.output_channel, configs.seq_len])
            self.layernorm_1_y = nn.LayerNorm(
                [
                    configs.output_channel,
                    configs.seq_len
                    - configs.input_token_len
                    + configs.output_token_len,
                ]
            )

            self.conv2 = nn.Conv1d(
                configs.output_channel,
                configs.input_channel,
                5,
                1,
                2,
                1,
                padding_mode="replicate",
            )
            self.layernorm_2_x = nn.LayerNorm([configs.input_channel, configs.seq_len])
            self.layernorm_2_y = nn.LayerNorm(
                [
                    configs.input_channel,
                    configs.seq_len
                    - configs.input_token_len
                    + configs.output_token_len,
                ]
            )

            self.silu = nn.SiLU()
            self.dropout = nn.Dropout1d(p=0.1)

        self.fc = nn.Linear(configs.input_channel, configs.input_channel)

    def forward(self, x, type=0):
        B, L, C = x.shape
        x = x.permute(0, 2, 1)  # B C L

        if self.configs.input_token_len == self.configs.output_token_len:
            x = self.conv1(x)
            x = self.conv2(x)
        else:
            x = self.conv1(x)
            if type == 0:
                x = self.layernorm_1_x(x)
            else:
                x = self.layernorm_1_y(x)
            x = self.silu(x)
            x = self.dropout(x)

            x = self.conv2(x)
            if type == 0:
                x = self.layernorm_2_x(x)
            else:
                x = self.layernorm_2_y(x)
            x = self.silu(x)
            x = self.dropout(x)

        x = x.permute(0, 2, 1)  # B L C
        x = self.fc(x)
        return x


class WeaverMLP(nn.Module):
    def __init__(self, configs):
        super().__init__()
        self.configs = configs
        self.fc1 = nn.Linear(configs.input_channel, configs.output_channel)
        self.fc2 = nn.Linear(configs.output_channel, configs.input_channel)
        self.silu = nn.SiLU()

    def forward(self, x):
        x = self.silu(self.fc1(x))
        x = self.fc2(x)
        return x


from torch.nn.parameter import Parameter


class DualWeaverSundialForPrediction(SundialPreTrainedModel):
    def __init__(self, config: DualWeaverSundialConfig):
        super().__init__(config)

        # Fix inference seed
        random.seed(2021)
        torch.manual_seed(2021)
        np.random.seed(2021)

        self.config = config
        self.ltm = SundialForPrediction(config)
        if "WeaverMLP" in config.feature_weaver:
            self.feature_weaver = WeaverMLP(config)
        elif "WeaverCNN" in config.feature_weaver:
            self.feature_weaver = WeaverCNN(config)
        else:
            raise NotImplementedError
        self.a = Parameter(torch.ones(1, config.input_channel))
        self.b = Parameter(torch.ones(1, config.input_channel))
        self.post_init()

    @torch.inference_mode()
    def generate(
        self,
        input_ids: torch.FloatTensor = None,
        max_new_tokens: int = 720,
        num_samples: int = 20,
    ):
        return self.forward(
            input_ids=input_ids,
            max_new_tokens=max_new_tokens,
            num_samples=num_samples,
        )

    def forward(
        self,
        input_ids: torch.FloatTensor = None,
        max_new_tokens: int = 720,
        num_samples: int = 20,
    ):
        batch_x = input_ids
        means = batch_x.mean(1, keepdim=True).detach()
        stdev = batch_x.std(dim=1, keepdim=True, unbiased=False).detach()
        stdev = torch.where(
            stdev > 1e-2, stdev, torch.tensor(1e-2, device=batch_x.device)
        )
        batch_x = (batch_x - means) / stdev

        x1 = self.a * batch_x + self.feature_weaver(batch_x)
        x2 = -self.b * batch_x + self.feature_weaver(batch_x)
        batch_x = torch.cat([x1, x2], dim=0)
        B = batch_x.shape[0]

        batch_x = batch_x[:, :, -1]
        # batch_x = batch_x.permute(0, 2, 1)  # B C L
        # batch_x = batch_x.reshape(-1, batch_x.shape[-1])

        predictions = self.ltm.generate(
            batch_x,
            max_new_tokens=max_new_tokens,
            num_samples=num_samples,
        )  # B N L
        # predictions = predictions.reshape(
        #     B, -1, predictions.shape[1], predictions.shape[-1]
        # )
        # predictions = predictions.permute(0, 2, 1, 3)  # B N C L
        # predictions = predictions.permute(0, 1, 3, 2)  # B N L C
        y1, y2 = torch.chunk(predictions, 2, dim=0)
        predictions = (y1 - y2) / (self.a[:, -1] + self.b[:, -1])
        predictions = predictions.permute(1, 0, 2)  # N B L
        predictions = predictions * stdev[:, :, -1] + means[:, :, -1]
        predictions = predictions.permute(1, 0, 2)  # B N L
        # predictions = predictions.mean(dim=0)
        return predictions
