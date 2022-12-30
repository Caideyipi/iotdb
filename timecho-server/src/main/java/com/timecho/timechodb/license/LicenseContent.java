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
package com.timecho.timechodb.license;

import java.util.ArrayList;

public class LicenseContent {
  private String cpu;
  private ArrayList<String> macs;
  private String mainBoard;
  private String expireDate;
  private long maxAllowedTimeSeriesNumber;
  private long maxInputFrequence;

  public String getCpu() {
    return cpu;
  }

  public void setCpu(String cpu) {
    this.cpu = cpu;
  }

  public ArrayList<String> getMacs() {
    return macs;
  }

  public void setMacs(ArrayList<String> macs) {
    this.macs = macs;
  }

  public String getMainBoard() {
    return mainBoard;
  }

  public void setMainBoard(String mainBoard) {
    this.mainBoard = mainBoard;
  }

  public String getExpireDate() {
    return expireDate;
  }

  public void setExpireDate(String expireDate) {
    this.expireDate = expireDate;
  }

  public long getMaxAllowedTimeSeriesNumber() {
    return maxAllowedTimeSeriesNumber;
  }

  public void setMaxAllowedTimeSeriesNumber(long maxAllowedTimeSeriesNumber) {
    this.maxAllowedTimeSeriesNumber = maxAllowedTimeSeriesNumber;
  }

  public long getMaxInputFrequence() {
    return maxInputFrequence;
  }

  public void setMaxInputFrequence(long maxInputFrequence) {
    this.maxInputFrequence = maxInputFrequence;
  }
}
