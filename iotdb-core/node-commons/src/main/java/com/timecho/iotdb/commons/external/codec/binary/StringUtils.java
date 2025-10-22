package com.timecho.iotdb.commons.external.codec.binary;

import org.apache.tsfile.external.commons.io.Charsets;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringUtils {
  /**
   * Constructs a new <code>String</code> by decoding the specified array of bytes using the UTF-8
   * charset.
   *
   * @param bytes The bytes to be decoded into characters
   * @return A new <code>String</code> decoded from the specified array of bytes using the UTF-8
   *     charset, or <code>null</code> if the input byte array was <code>null</code>.
   * @throws NullPointerException Thrown if {@link Charsets#UTF_8} is not initialized, which should
   *     never happen since it is required by the Java platform specification.
   * @since As of 1.7, throws {@link NullPointerException} instead of UnsupportedEncodingException
   */
  public static String newStringUtf8(final byte[] bytes) {
    return newString(bytes, Charsets.UTF_8);
  }

  /**
   * Constructs a new <code>String</code> by decoding the specified array of bytes using the given
   * charset.
   *
   * @param bytes The bytes to be decoded into characters
   * @param charset The {@link Charset} to encode the <code>String</code>; not {@code null}
   * @return A new <code>String</code> decoded from the specified array of bytes using the given
   *     charset, or <code>null</code> if the input byte array was <code>null</code>.
   * @throws NullPointerException Thrown if charset is {@code null}
   */
  private static String newString(final byte[] bytes, final Charset charset) {
    return bytes == null ? null : new String(bytes, charset);
  }

  /**
   * Constructs a new {@code String} by decoding the specified array of bytes using the US-ASCII
   * charset.
   *
   * @param bytes The bytes to be decoded into characters
   * @return A new {@code String} decoded from the specified array of bytes using the US-ASCII
   *     charset, or {@code null} if the input byte array was {@code null}.
   * @throws NullPointerException Thrown if {@link StandardCharsets#US_ASCII} is not initialized,
   *     which should never happen since it is required by the Java platform specification.
   * @since As of 1.7, throws {@link NullPointerException} instead of UnsupportedEncodingException
   */
  public static String newStringUsAscii(final byte[] bytes) {
    return newString(bytes, StandardCharsets.US_ASCII);
  }
}
