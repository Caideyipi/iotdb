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

echo ```````````````````````````
echo Removing IoTDB MLNode
echo ```````````````````````````

call %~dp0..\\conf\\mlnode-env.bat %*
if %errorlevel% neq 0 (
    echo Environment check failed. Exiting...
    exit /b 1
)

for /f "tokens=2 delims==" %%a in ('findstr /i /c:"^mln_interpreter_dir" "%~dp0..\\conf\\mlnode-env.bat"') do (
    set _mln_interpreter_dir=%%a
    goto :interpreter
)

:interpreter
if "%_mln_interpreter_dir%"=="" (
    set _mln_interpreter_dir=%~dp0..\\venv\\Scripts\\python.exe
)

for /f "tokens=2 delims==" %%a in ('findstr /i /c:"^mln_system_dir" "%~dp0..\\conf\\iotdb-mlnode.properties"') do (
    set _mln_system_dir=%%a
    goto :system
)

:system
if "%_mln_system_dir%"=="" (
    set _mln_system_dir=%~dp0..\\data\\mlnode\\system
)

echo Script got parameters: mln_interpreter_dir: %_mln_interpreter_dir%, mln_system_dir: %_mln_system_dir%
cd %~dp0..
for %%i in ("%_mln_interpreter_dir%") do set "parent=%%~dpi"
set mln_mlnode_dir=%parent%\\mlnode.exe

%mln_mlnode_dir% remove

if %errorlevel% neq 0 (
    echo Remove MLNode failed. Exiting...
    exit /b 1
)

call %~dp0\\stop-mlnode.bat

rd /s /q %_mln_system_dir%

pause