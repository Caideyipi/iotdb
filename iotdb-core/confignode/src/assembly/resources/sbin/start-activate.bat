@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@echo off

pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

if exist "%IOTDB_HOME%\activation\license" (
    echo %IOTDB_HOME%\activation\license exists, which means this ConfigNode may have been activated.
    echo "Do you want to continue (the current license file will be overwritten) ? y/N"
    set /p continue_activation=

    echo - - - - - - - - - -

    if "%continue_activation%"=="y" (
        echo Continue activating...
    ) else (
        echo End activating
        exit /b 0
    )
)

if exist "%IOTDB_HOME%\activation\system_info" (
    echo Please copy the system_info's content and send it to Timecho:
    type "%IOTDB_HOME%\activation\system_info"
    echo.
) else (
    echo %IOTDB_HOME%\activation\system_info not exist, please start ConfigNode first to create this file.
    exit /b 1
)

echo Please enter license:
set /p activation_code=

echo - - - - - - - - - -

echo %activation_code% > "%IOTDB_HOME%\activation\license"
echo License has been stored to %IOTDB_HOME%\activation\license

echo This ConfigNode's activation process has completed.
echo After activating other ConfigNodes, please start the cluster and use 'show cluster' to verify whether the activation is successful.