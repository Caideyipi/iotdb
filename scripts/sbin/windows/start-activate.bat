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

pushd %~dp0..\..\
if NOT DEFINED CONFIGNODE_HOME set CONFIGNODE_HOME=%cd%
popd

if NOT DEFINED CONFIGNODE_CONF set CONFIGNODE_CONF=%CONFIGNODE_HOME%/conf

set "LICENSE_PATH=%CONFIGNODE_HOME%\activation\license"

if exist "%LICENSE_PATH%" (
    echo WARNING: %LICENSE_PATH% exists, which means this ConfigNode may have been activated.
    echo WARNING: The following activation process will overwrite the current license file.
    timeout /t 1 >nul
    echo - - - - - - - - - -
    timeout /t 1 >nul
)

set "SYSTEM_INFO_FILE_PATH=%CONFIGNODE_HOME%\activation\system_info"

if not exist "%SYSTEM_INFO_FILE_PATH%" (
    echo %SYSTEM_INFO_FILE_PATH% not exist, please start ConfigNode first to create this file.
    exit /b 1
)

echo Please copy the system_info's content and send it to Timecho:
type "%SYSTEM_INFO_FILE_PATH%"
echo.

echo Please enter license:
set /p activation_code=

echo %activation_code% >"%LICENSE_PATH%"
timeout /t 1 >nul
echo - - - - - - - - - -
timeout /t 1 >nul
echo License has been successfully stored to %LICENSE_PATH%
echo Import completed. Starting to verify the license...
echo - - - - - - - - - -

set "activation_main_class=com.timecho.iotdb.manager.activation.ActivationVerifier"
set "iotdb_params=-DCONFIGNODE_CONF=%CONFIGNODE_CONF%"
set "iotdb_params=%iotdb_params% -DCONFIGNODE_HOME=%CONFIGNODE_HOME%"

set CLASSPATH="%CONFIGNODE_HOME%\lib\*"

java %iotdb_params% -cp "%CLASSPATH%" "%activation_main_class%" "%LICENSE_PATH%"

exit /b
