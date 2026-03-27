from iotdb.ainode.core.model.sundial.configuration_sundial import SundialConfig


class DualWeaverSundialConfig(SundialConfig):
    model_type = "dual_weaver_sundial"

    def __init__(
        self,
        feature_weaver: str = "WeaverCNN",
        input_channel: int = 17,
        output_channel: int = 64,
        output_token_len: int = 720,
        seq_len: int = 2880,
        **kwargs,
    ):
        self.feature_weaver = feature_weaver
        self.input_channel = input_channel
        self.output_channel = output_channel
        self.output_token_len = output_token_len
        self.seq_len = seq_len

        super().__init__(
            **kwargs,
        )
