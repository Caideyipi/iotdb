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
package com.timecho.timechodb.utils;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.util.StrUtil;
import com.timecho.timechodb.license.LicenseException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil {
  public static int timeSub(String early, String late) throws LicenseException {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      Long earlyTimeStamp = sdf.parse(early).getTime();
      Long lateTimeStamp = sdf.parse(late).getTime();
      return (int) ((lateTimeStamp - earlyTimeStamp) / (1000 * 3600 * 24));
    } catch (Exception e) {
      throw new LicenseException("date parse error");
    }
  }

  public static String format(Date date, String format) {
    if (null != date && !StrUtil.isBlank(format)) {
      SimpleDateFormat sdf = new SimpleDateFormat(format);
      if (date instanceof DateTime) {
        TimeZone timeZone = ((DateTime) date).getTimeZone();
        if (null != timeZone) {
          sdf.setTimeZone(timeZone);
        }
      }

      return format(date, sdf);
    } else {
      return null;
    }
  }

  public static String format(Date date, DateFormat format) {
    return null != format && null != date ? format.format(date) : null;
  }
}
