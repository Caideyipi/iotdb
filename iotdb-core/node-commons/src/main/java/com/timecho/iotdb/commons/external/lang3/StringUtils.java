package com.timecho.iotdb.commons.external.lang3;

import java.util.Locale;

import static org.apache.tsfile.external.commons.lang3.StringUtils.EMPTY;
import static org.apache.tsfile.external.commons.lang3.StringUtils.isEmpty;
import static org.apache.tsfile.external.commons.lang3.Strings.INDEX_NOT_FOUND;

public class StringUtils {
  /**
   * Gets the substring before the first occurrence of a separator. The separator is not returned.
   *
   * <p>A {@code null} string input will return {@code null}. An empty ("") string input will return
   * the empty string. A {@code null} separator will return the input string.
   *
   * <p>If nothing is found, the string input is returned.
   *
   * <pre>
   * StringUtils.substringBefore(null, *)      = null
   * StringUtils.substringBefore("", *)        = ""
   * StringUtils.substringBefore("abc", "a")   = ""
   * StringUtils.substringBefore("abcba", "b") = "a"
   * StringUtils.substringBefore("abc", "c")   = "ab"
   * StringUtils.substringBefore("abc", "d")   = "abc"
   * StringUtils.substringBefore("abc", "")    = ""
   * StringUtils.substringBefore("abc", null)  = "abc"
   * </pre>
   *
   * @param str the String to get a substring from, may be null
   * @param separator the String to search for, may be null
   * @return the substring before the first occurrence of the separator, {@code null} if null String
   *     input
   * @since 2.0
   */
  public static String substringBefore(final String str, final String separator) {
    if (isEmpty(str) || separator == null) {
      return str;
    }
    if (separator.isEmpty()) {
      return EMPTY;
    }
    final int pos = str.indexOf(separator);
    if (pos == INDEX_NOT_FOUND) {
      return str;
    }
    return str.substring(0, pos);
  }

  /**
   * Converts a String to upper case as per {@link String#toUpperCase()}.
   *
   * <p>A {@code null} input String returns {@code null}.
   *
   * <pre>
   * StringUtils.upperCase(null)  = null
   * StringUtils.upperCase("")    = ""
   * StringUtils.upperCase("aBc") = "ABC"
   * </pre>
   *
   * <p><strong>Note:</strong> As described in the documentation for {@link String#toUpperCase()},
   * the result of this method is affected by the current locale. For platform-independent case
   * transformations, the method {@link #upperCase(String, Locale)} should be used with a specific
   * locale (e.g. {@link Locale#ENGLISH}).
   *
   * @param str the String to upper case, may be null
   * @return the upper-cased String, {@code null} if null String input
   */
  public static String upperCase(final String str) {
    if (str == null) {
      return null;
    }
    return str.toUpperCase();
  }
}
