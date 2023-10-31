# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from typing import cast

import psutil

from iotdb.mlnode.constant import TaskType, TSStatusCode
from iotdb.mlnode.dataset.dataset import TsForecastDataset
from iotdb.mlnode.dataset.factory import create_dataset
from iotdb.mlnode.exception import InvaildUriError, BadConfigValueError
from iotdb.mlnode.inference import inference
from iotdb.mlnode.log import logger
from iotdb.mlnode.parser import (ForecastTaskOptions,
                                 parse_task_options, parse_inference_request)
from iotdb.mlnode.serde import convert_to_binary
from iotdb.mlnode.storage import model_storage
from iotdb.mlnode.util import get_status
from iotdb.thrift.common.ttypes import TLoadSample
from iotdb.thrift.mlnode import IMLNodeRPCService
from iotdb.thrift.mlnode.ttypes import (TCreateTrainingTaskReq,
                                        TDeleteModelReq, TRegisterModelReq,
                                        TRegisterModelResp, TConfigs,
                                        TMlHeartbeatReq, TMLHeartbeatResp,
                                        TInferenceReq, TInferenceResp)


class MLNodeRPCServiceHandler(IMLNodeRPCService.Iface):
    def __init__(self):
        # for training, it's not open now.
        self.__task_manager = None

    def registerModel(self, req: TRegisterModelReq):
        logger.debug(f"register model {req.modelId} from {req.uri}")
        try:
            configs, attributes = model_storage.register_model(req.modelId, req.uri)
            return TRegisterModelResp(get_status(TSStatusCode.SUCCESS_STATUS), configs, attributes)
        except InvaildUriError as e:
            logger.warning(e)
            model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.INVALID_URI_ERROR), TConfigs())
        except BadConfigValueError as e:
            logger.warning(e)
            model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.INVALID_INFERENCE_CONFIG),TConfigs())
        except Exception as e:
            logger.warning(e)
            model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.MLNODE_INTERNAL_ERROR),TConfigs())

    def deleteModel(self, req: TDeleteModelReq):
        logger.debug(f"delete model {req.modelId}")
        try:
            model_storage.delete_model(req.modelId)
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warning(e)
            return get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e))

    def createTrainingTask(self, req: TCreateTrainingTaskReq):
        logger.debug(f"create training task {req.modelId}")
        task = None
        try:
            # parse options
            task_options = parse_task_options(req.options)
            dataset = create_dataset(req.datasetFetchSQL, task_options)

            # create task according to task type
            # currently, IoTDB-ML supports forecasting training task only
            if task_options.get_task_type() == TaskType.FORECAST:
                task_options = cast(ForecastTaskOptions, task_options)
                dataset = cast(TsForecastDataset, dataset)
                task = self.__task_manager.create_forecast_training_task(
                    model_id=req.modelId,
                    task_options=task_options,
                    hyperparameters=req.hyperparameters,
                    dataset=dataset
                )
            else:
                raise NotImplementedError

            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warning(e)
            return get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e))
        finally:
            if task is not None:
                # submit task to process pool
                self.__task_manager.submit_training_task(task)

    def inference(self, req: TInferenceReq):
        logger.info(f"infer {req.modelId}")
        model_id, full_data, window_interval, window_step = parse_inference_request(
            req)
        try:
            inference_results = inference(
                model_id, full_data, window_interval, window_step)
            for i in range(len(inference_results)):
                inference_results[i] = convert_to_binary(inference_results[i])
            return TInferenceResp(
                get_status(
                    TSStatusCode.SUCCESS_STATUS),
                inference_results)
        except Exception as e:
            logger.warning(e)
            return TInferenceResp(get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e)), inference_results)

    def getMLHeartbeat(self, req: TMlHeartbeatReq):
        if req.needSamplingLoad:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            disk_usage = psutil.disk_usage('/')
            disk_free = disk_usage.free
            load_sample = TLoadSample(cpuUsageRate=cpu_percent,
                                      memoryUsageRate=memory_percent,
                                      diskUsageRate=disk_usage.percent,
                                      freeDiskSpace=disk_free / 1024 / 1024 / 1024)
            return TMLHeartbeatResp(heartbeatTimestamp=req.heartbeatTimestamp,
                                    status="Running",
                                    loadSample=load_sample)
        else:
            return TMLHeartbeatResp(heartbeatTimestamp=req.heartbeatTimestamp,
                                    status="Running")
