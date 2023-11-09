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

@REM The defaulte venv environment is used if mln_interpreter_dir is not set. Please use absolute path without quotation mark
@REM set mln_interpreter_dir=

@REM Set mln_force_reinstall to 1 to force reinstall MLNode
set mln_force_reinstall=0

set ENV_SCRIPT_DIR=%~dp0

:initial
if "%1"=="" goto done
set aux=%1
if "%aux:~0,2%"=="-r" (
    set mln_force_reinstall=1
    shift
    goto initial
)
if "%aux:~0,1%"=="-" (
   set nome=%aux:~1,250%
) else (
   set "%nome%=%1"
   set nome=
)
shift
goto initial

:done
@REM check if the parameters are set
if "%i%"=="" (
    echo No interpreter_dir is set, use default value.
) else (
    set mln_interpreter_dir=%i%
)

echo Script got inputs: mln_interpreter_dir: %mln_interpreter_dir% , mln_force_reinstall: %mln_force_reinstall%
if "%mln_interpreter_dir%"=="" (
    %ENV_SCRIPT_DIR%//..//venv//Scripts//python.exe -c "import sys; print(sys.executable)" && (
        echo Activate default venv environment
    ) || (
        echo Creating default venv environment
        python -m venv "%ENV_SCRIPT_DIR%//..//venv"
    )
    set mln_interpreter_dir="%ENV_SCRIPT_DIR%//..//venv//Scripts//python.exe"
)

@REM Switch the working directory to the directory one level above the script
cd %ENV_SCRIPT_DIR%/../

echo Confirming mlnode
%mln_interpreter_dir% -m pip list | findstr /C:"apache-iotdb-mlnode" >nul
if %errorlevel% == 0 (
    if %mln_force_reinstall% == 0 (
        echo MLNode is already installed
        exit /b 0
    )
)

echo Installing MLNode...
@REM Print current work dir
cd lib
for %%i in (*.whl) do (
    @REM if mln_force_reinstall is 1 then force reinstall MLNode
    if %mln_force_reinstall% == 1 (
        echo Force reinstall %%i
        %mln_interpreter_dir% -m pip install %%i --force-reinstall -i https://pypi.tuna.tsinghua.edu.cn/simple --no-warn-script-location
    ) else (
        %mln_interpreter_dir% -m pip install %%i -i https://pypi.tuna.tsinghua.edu.cn/simple --no-warn-script-location
    )
    if %errorlevel% == 0 (
        echo MLNode is installed successfully
        cd ..
        exit /b 0
    )
)

echo Failed to install MLNode
exit /b 1