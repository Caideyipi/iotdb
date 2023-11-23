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
import numpy as np
import pandas as pd
from sktime.annotation.hmm_learn import GaussianHMM, GMMHMM
from sktime.forecasting.arima import ARIMA
from sktime.forecasting.exp_smoothing import ExponentialSmoothing
from sktime.forecasting.naive import NaiveForecaster
from sktime.forecasting.trend import STLForecaster
from torch import tensor

from iotdb.mlnode.attribute import parse_attribute, get_model_attributes, get_task_type
from iotdb.mlnode.constant import BuiltInModelType, TaskType
from iotdb.mlnode.exception import InvalidWindowArgumentError, InferenceModelInternalError, \
    BuiltInModelNotSupportError, AttributeNotSupportError
from iotdb.mlnode.log import logger
from iotdb.mlnode.storage import model_storage


def inference_with_registered_model(model_id, full_data, window_interval, window_step, inference_attributes):
    """
    Args:
        model_id: the unique id of the model
        full_data: a tuple of (data, time_stamp, type_list, column_name_list), where the data is a DataFrame with shape
            (L, C), time_stamp is a DataFrame with shape(L, 1), type_list is a list of data types with length C,
            column_name_list is a list of column names with length C, where L is the number of data points, C is the
            number of variables, the data and time_stamp are aligned by index
        window_interval: the length of each sliding window
        window_step: the step between two adjacent sliding windows
        inference_attributes: a list of attributes to be inferred. In this function, the attributes will include the
            acceleration, which indicates whether the model is accelerated by the torch. Compile
    Returns:
        outputs: a list of output DataFrames, where each DataFrame has shape (H', C'), where H' is the output window
            interval, C' is the number of variables in the output DataFrame
    Description:
        the inference_with_registered_model function will inference with deep learning model, which is registered in
        user register process. This module will split the input data into several sliding windows which has the same
        shape (1, H, C), where H is the window interval, and then feed each sliding window into the model to get the
        output, the output is a DataFrame with shape (H', C'), where H' is the output window interval, C' is the number
        of variables in the output DataFrame. Then the inference module will concatenate all the output DataFrames into
        a list.
    """
    logger.info(f"start inference registered model {model_id}")

    # parse the inference attributes
    acceleration = False
    if inference_attributes is None or 'acceleration' not in inference_attributes:
        # if the acceleration is not specified, then the acceleration will be set to default value False
        acceleration = False
    else:
        # if the acceleration is specified, then the acceleration will be set to the specified value
        acceleration = (inference_attributes['acceleration'].lower() == 'true')

    model = model_storage.load_model_from_id(model_id, acceleration)
    dataset, dataset_length = process_data(full_data)

    # check the validity of window_interval and window_step, the two arguments must be positive integers, and the
    # window_interval should not be larger than the dataset length
    if window_interval is None or window_step is None \
            or window_interval > dataset_length \
            or window_interval <= 0 or \
            window_step <= 0:
        raise InvalidWindowArgumentError(window_interval, window_step, dataset_length)

    sliding_times = int((dataset_length - window_interval) // window_step + 1)
    outputs = []
    try:
        # split the input data into several sliding windows
        for sliding_time in range(sliding_times):
            if window_step == float('inf'):
                start_index = 0
            else:
                start_index = sliding_time * window_step
            end_index = start_index + window_interval
            # input_data: tensor, shape: (1, H, C), where H is input window interval
            input_data = dataset[:, start_index:end_index, :]
            # output: tensor, shape: (1, H', C'), where H' is the output window interval
            output = model(input_data)
            # output: DataFrame, shape: (H', C')
            output = pd.DataFrame(output.squeeze(0).detach().numpy())
            outputs.append(output)
    except Exception as e:
        raise InferenceModelInternalError(str(e))

    return outputs


def process_data(full_data):
    """
    Args:
        full_data: a tuple of (data, time_stamp, type_list, column_name_list), where the data is a DataFrame with shape
            (L, C), time_stamp is a DataFrame with shape(L, 1), type_list is a list of data types with length C,
            column_name_list is a list of column names with length C, where L is the number of data points, C is the
            number of variables, the data and time_stamp are aligned by index
    Returns:
        data: a tensor with shape (1, L, C)
        data_length: the number of data points
    Description:
        the process_data module will convert the input data into a tensor with shape (1, L, C), where L is the number of
        data points, C is the number of variables, the data and time_stamp are aligned by index. The module will also
        convert the data type of each column to the corresponding type.
    """
    data, time_stamp, type_list, _ = full_data
    data_length = time_stamp.shape[0]
    data = data.fillna(0)
    for i in range(len(type_list)):
        if type_list[i] == "TEXT":
            data[data.columns[i]] = 0
        elif type_list[i] == "BOOLEAN":
            data[data.columns[i]] = data[data.columns[i]].astype("int")
    data = tensor(data.values).unsqueeze(0)
    return data, data_length


def inference_with_built_in_model(model_id, full_data, inference_attributes):
    """
    Args:
        model_id: the unique id of the model
        full_data: a tuple of (data, time_stamp, type_list, column_name_list), where the data is a DataFrame with shape
            (L, C), time_stamp is a DataFrame with shape(L, 1), type_list is a list of data types with length C,
            column_name_list is a list of column names with length C, where L is the number of data points, C is the
            number of variables, the data and time_stamp are aligned by index
        inference_attributes: a list of attributes to be inferred, in this function, the attributes will include some
            parameters of the built-in model. Some parameters are optional, and if the parameters are not
            specified, the default value will be used.
    Returns:
        outputs: a list of output DataFrames, where each DataFrame has shape (H', C'), where H' is the output window
            interval, C' is the number of variables in the output DataFrame
    Description:
        the inference_with_built_in_model function will inference with built-in model from sktime, which does not
        require user registration. This module will parse the inference attributes and create the built-in model, then
        feed the input data into the model to get the output, the output is a DataFrame with shape (H', C'), where H'
        is the output window interval, C' is the number of variables in the output DataFrame. Then the inference module
        will concatenate all the output DataFrames into a list.
    """
    model_id = model_id.lower()
    if model_id not in BuiltInModelType.values():
        raise BuiltInModelNotSupportError(model_id)
    task_type = get_task_type(model_id)

    logger.info(f"start inference built-in model {model_id}")

    # parse the inference attributes and create the built-in model
    model, attributes = create_built_in_model(model_id, inference_attributes)

    data, _, _, _ = full_data
    try:
        model.fit(data)
        # inference
        if task_type == TaskType.FORECAST.value:
            predict_len = attributes['predict_length']
            output = model.predict(fh=range(predict_len))
            output = np.array(output, dtype=np.float64)
        elif task_type == TaskType.ANOMALY_DETECTION.value:
            output = model.predict(data)
            output = np.array(output, dtype=np.int32)
        else:
            raise BuiltInModelNotSupportError(model_id)
    except Exception as e:
        raise InferenceModelInternalError(f"model inference error: {str(e)}")

    # output: DataFrame, shape: (H', C')
    output = pd.DataFrame(output)
    outputs = [output]
    return outputs


def create_built_in_model(model_id, inference_attributes):
    """
    Args:
        model_id: the unique id of the model
        inference_attributes: a list of attributes to be inferred, in this function, the attributes will include some
            parameters of the built-in model. Some parameters are optional, and if the parameters are not
            specified, the default value will be used.
    Returns:
        model: the built-in model from sktime
        attributes: a dict of attributes, where the key is the attribute name, the value is the parsed value of the
            attribute
    Description:
        the create_built_in_model function will create the built-in model from sktime, which does not require user
        registration. This module will parse the inference attributes and create the built-in model.
    """
    attribute_map = get_model_attributes(model_id)

    # validate the inference attributes
    for attribute_name in inference_attributes:
        if attribute_name not in attribute_map:
            raise AttributeNotSupportError(model_id, attribute_name)

    # parse the inference attributes, attributes is a Dict[str, Any]
    attributes = parse_attribute(inference_attributes, attribute_map)

    # build the built-in model
    model = None
    if model_id == BuiltInModelType.ARIMA.value:
        model = ARIMA(
            order=attributes['order'],
            seasonal_order=attributes['seasonal_order'],
            method=attributes['method'],
            suppress_warnings=attributes['suppress_warnings'],
            maxiter=attributes['maxiter'],
            out_of_sample_size=attributes['out_of_sample_size'],
            scoring=attributes['scoring'],
            with_intercept=attributes['with_intercept'],
            time_varying_regression=attributes['time_varying_regression'],
            enforce_stationarity=attributes['enforce_stationarity'],
            enforce_invertibility=attributes['enforce_invertibility'],
            simple_differencing=attributes['simple_differencing'],
            measurement_error=attributes['measurement_error'],
            mle_regression=attributes['mle_regression'],
            hamilton_representation=attributes['hamilton_representation'],
            concentrate_scale=attributes['concentrate_scale']
        )
    elif model_id == BuiltInModelType.EXPONENTIAL_SMOOTHING.value:
        model = ExponentialSmoothing(
            damped_trend=attributes['damped_trend'],
            initialization_method=attributes['initialization_method'],
            optimized=attributes['optimized'],
            remove_bias=attributes['remove_bias'],
            use_brute=attributes['use_brute']
        )
    elif model_id == BuiltInModelType.NAIVE_FORECASTER.value:
        model = NaiveForecaster(
            strategy=attributes['strategy'],
            sp=attributes['sp']
        )
    elif model_id == BuiltInModelType.STL_FORECASTER.value:
        model = STLForecaster(
            sp=attributes['sp'],
            seasonal=attributes['seasonal'],
            seasonal_deg=attributes['seasonal_deg'],
            trend_deg=attributes['trend_deg'],
            low_pass_deg=attributes['low_pass_deg'],
            seasonal_jump=attributes['seasonal_jump'],
            trend_jump=attributes['trend_jump'],
            low_pass_jump=attributes['low_pass_jump']
        )
    elif model_id == BuiltInModelType.GMM_HMM.value:
        model = GMMHMM(
            n_components=attributes['n_components'],
            n_mix=attributes['n_mix'],
            min_covar=attributes['min_covar'],
            startprob_prior=attributes['startprob_prior'],
            transmat_prior=attributes['transmat_prior'],
            means_prior=attributes['means_prior'],
            means_weight=attributes['means_weight'],
            weights_prior=attributes['weights_prior'],
            algorithm=attributes['algorithm'],
            covariance_type=attributes['covariance_type'],
            n_iter=attributes['n_iter'],
            tol=attributes['tol'],
            params=attributes['params'],
            init_params=attributes['init_params'],
            implementation=attributes['implementation']
        )
    elif model_id == BuiltInModelType.GAUSSIAN_HMM.value:
        model = GaussianHMM(
            n_components=attributes['n_components'],
            covariance_type=attributes['covariance_type'],
            min_covar=attributes['min_covar'],
            startprob_prior=attributes['startprob_prior'],
            transmat_prior=attributes['transmat_prior'],
            means_prior=attributes['means_prior'],
            means_weight=attributes['means_weight'],
            covars_prior=attributes['covars_prior'],
            covars_weight=attributes['covars_weight'],
            algorithm=attributes['algorithm'],
            n_iter=attributes['n_iter'],
            tol=attributes['tol'],
            params=attributes['params'],
            init_params=attributes['init_params'],
            implementation=attributes['implementation']
        )
    else:
        raise BuiltInModelNotSupportError(model_id)

    return model, attributes
