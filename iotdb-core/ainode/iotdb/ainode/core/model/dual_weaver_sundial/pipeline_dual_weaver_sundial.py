import torch

from iotdb.ainode.core.exception import InferenceModelInternalException
from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.model.model_info import ModelInfo


class DualWeaverSundialPipeline(ForecastPipeline):

    def __init__(self, model_info: ModelInfo, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def _preprocess(self, inputs, **infer_kwargs) -> torch.Tensor:
        model_id = self.model_info.model_id
        covariates = self.model.config.input_channel - 1
        seq_len = self.model.config.seq_len

        # Exactly 1 target variable
        targets = inputs[0].get("targets")
        if targets.shape[0] > 1:
            raise InferenceModelInternalException(
                f"Model {model_id} accepts exactly 1 target variable, but receives {targets.shape[0]} target variables."
            )

        # Ensure the number of covariates
        history_covs = inputs[0].get("past_covariates", None)
        num_history_covs = len(history_covs) if history_covs is not None else 0
        if num_history_covs != covariates:
            raise InferenceModelInternalException(
                f"Model {model_id} requires exactly {covariates} covariates, but receives {num_history_covs} covariates."
            )
        future_covs = inputs[0].get("future_covariates", None)
        num_future_covs = len(future_covs) if future_covs is not None else 0
        if num_future_covs != covariates:
            raise InferenceModelInternalException(
                f"Model {model_id} requires exactly {covariates} covariates, but receives {num_future_covs} covariates."
            )

        history_covs = torch.stack([history_covs[k] for k in history_covs.keys()])
        future_covs = torch.stack([future_covs[k] for k in future_covs.keys()])
        cov_tensor = (
            torch.cat([history_covs, future_covs], dim=1)[:, -seq_len:]
            .unsqueeze(0)
            .float()
        )
        targets = targets.unsqueeze(1).float()

        inputs = torch.cat([cov_tensor, targets], dim=1).permute(0, 2, 1)
        return inputs

    def forecast(self, inputs: torch.Tensor, **infer_kwargs) -> torch.Tensor:
        output_length = infer_kwargs.get("output_length", None)
        if output_length is None:
            raise InferenceModelInternalException(
                f"Model {self.model_info.model_id} requires parameter 'output_length' for forecasting but found None."
            )
        return self.model.generate(input_ids=inputs, max_new_tokens=output_length)

    def _postprocess(self, outputs: torch.Tensor, **infer_kwargs) -> list[torch.Tensor]:
        output_length = infer_kwargs.get("output_length")
        output = outputs[0].mean(dim=0)[:output_length].unsqueeze(0)
        return [output]
