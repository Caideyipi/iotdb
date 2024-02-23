/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

parser grammar PathParser;

options { tokenVocab=PathLexer; }


/**
 * PartialPath and Path used by Session API and TsFile API should be parsed by Antlr4.
 */

path
    : prefixPath EOF
    | suffixPath EOF
    ;

prefixPatternPath
    : ROOT (DOT nodeNameWithoutWildcard)* (DOT prefixNodeNameSlice?)? EOF
    ;

prefixPath
    : ROOT (DOT nodeName)*
    ;

suffixPath
    : nodeName (DOT nodeName)*
    ;

nodeName
    : wildcard
    | wildcard nodeNameSlice wildcard?
    | nodeNameSlice wildcard
    | nodeNameWithoutWildcard
    ;

nodeNameWithoutWildcard
    : identifier
    ;

prefixNodeNameSlice
    : LEFT_QUOTED_ID
    | nodeNameSlice
    ;

nodeNameSlice
    : identifier
    | INTEGER_LITERAL
    ;

identifier
     : DURATION_LITERAL
     | ID
     | QUOTED_ID
     ;

wildcard
    : STAR
    | DOUBLE_STAR
    ;