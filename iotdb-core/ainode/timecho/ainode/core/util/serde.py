import numpy as np
import torch

from iotdb.tsfile.utils.tsblock_serde import deserialize


# Full data deserialized from iotdb tsblock is composed of [timestampList, multiple valueList, None, length].
def convert_tsblock_to_tensor_and_timestamps(
    tsblock_data: bytes,
) -> tuple[torch.Tensor, list[int]]:
    full_data = deserialize(tsblock_data)
    # ensure the byteorder is correct.
    for i, data in enumerate(full_data[1]):
        if data.dtype.byteorder not in ("=", "|"):
            np_data = data.byteswap()
            full_data[1][i] = np_data.view(np_data.dtype.newbyteorder())
    # tensor_data: [batch_size, target_count, input_length]
    tensor_data = torch.from_numpy(np.stack(full_data[1], axis=0)).unsqueeze(0).float()
    # timestamps: [input_length,]
    timestamps: np.ndarray = full_data[0]
    # data should be on CPU before passing to the inference request
    return tensor_data.to("cpu"), timestamps.tolist()
