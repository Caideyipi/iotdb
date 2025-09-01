import torch


def get_available_gpus():
    """
    Returns a list of available GPU indices if CUDA is available, otherwise returns an empty list.
    """

    if not torch.cuda.is_available():
        return []
    return list(range(torch.cuda.device_count()))


def parse_devices(devices):
    """
    Parses the input string of GPU devices and returns a comma-separated string of valid GPU indices.

    Args:
        devices (str): A comma-separated string of GPU indices (e.g., "0,1,2").
    Returns:
        str: A comma-separated string of valid GPU indices corresponding to the input. All available GPUs if no input is provided.
    Exceptions:
        RuntimeError: If no GPUs are available.
        ValueError: If any of the provided GPU indices are not available.
    """
    if devices is None or devices == "":
        gpu_ids = get_available_gpus()
        if not gpu_ids:
            raise RuntimeError("No available GPU")
        return ",".join(map(str, gpu_ids))
    else:
        gpu_ids = [int(gpu) for gpu in devices.split(",")]
        available_gpus = get_available_gpus()
        for gpu_id in gpu_ids:
            if gpu_id not in available_gpus:
                raise ValueError(
                    f"GPU {gpu_id} is not available, the available choices are: {available_gpus}"
                )
        return devices
