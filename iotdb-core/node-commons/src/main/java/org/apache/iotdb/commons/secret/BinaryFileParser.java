package org.apache.iotdb.commons.secret;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Binary file parsing and writing utility class Provides functions for reading, parsing, and
 * writing binary files
 */
public class BinaryFileParser {

  // ==================== File Reading Methods ====================

  /** Read binary file as byte array */
  public static byte[] readBinaryFile(String filePath) throws IOException {
    try (FileInputStream fis = new FileInputStream(filePath);
        ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = fis.read(buffer)) != -1) {
        bos.write(buffer, 0, bytesRead);
      }
      return bos.toByteArray();
    }
  }

  // ==================== File Writing Methods ====================

  /**
   * Write byte array to binary file
   *
   * @param filePath File path
   * @param data Byte array data
   * @param append Whether to use append mode
   */
  public static void writeBytesToFile(String filePath, byte[] data, boolean append)
      throws IOException {
    try (FileOutputStream fos = new FileOutputStream(filePath, append)) {
      fos.write(data);
    }
  }

  public static void writeBytesToFile(String filePath, byte[] data) throws IOException {
    writeBytesToFile(filePath, data, false);
  }

  /**
   * Write string as encrypted binary to file, cannot directly view original content Effect is
   * similar to writeIntToFile, shows as garbled text when opened in text editor
   */
  public static void writeStringAsBinary(
      String filePath, String text, boolean append, ByteOrder byteOrder) throws IOException {
    if (text == null || text.isEmpty()) {
      throw new IllegalArgumentException("Text content cannot be empty");
    }

    // Convert text to byte array
    byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);

    // Calculate total bytes: random number(4) + key(1) + encrypted text + checksum byte(1) +
    // length(4) = 10 + textBytes.length
    int totalSize = 10 + textBytes.length;

    // Create ByteBuffer with correct size
    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(byteOrder);

    // Add some random bytes as interference
    Random random = new Random();
    buffer.putInt(random.nextInt());

    // Simple XOR encryption for text bytes
    byte xorKey = (byte) random.nextInt(256);
    buffer.put(xorKey);

    // Write encrypted text
    for (int i = 0; i < textBytes.length; i++) {
      buffer.put((byte) (textBytes[i] ^ xorKey));
    }

    // Add checksum byte and end marker
    buffer.put((byte) (textBytes.length ^ 0xFF));
    buffer.putInt(textBytes.length);

    // Ensure all data has been written
    if (buffer.position() != totalSize) {
      throw new IOException("Buffer write incomplete");
    }

    writeBytesToFile(filePath, buffer.array(), append);
  }

  /**
   * Simplified string writing method (not encrypted, but still shows as garbled text in text
   * editor)
   */
  public static void writeStringAsRawBinary(
      String filePath, String text, boolean append, ByteOrder byteOrder) throws IOException {
    if (text == null || text.isEmpty()) {
      throw new IllegalArgumentException("Text content cannot be empty");
    }

    // Convert text to byte array
    byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);

    // Create ByteBuffer and write length information
    ByteBuffer buffer = ByteBuffer.allocate(4 + textBytes.length).order(byteOrder);
    buffer.putInt(textBytes.length);
    buffer.put(textBytes);

    writeBytesToFile(filePath, buffer.array(), append);
  }

  /** Read string from encrypted binary file */
  public static String readStringFromBinary(String filePath, ByteOrder byteOrder)
      throws IOException {
    byte[] data = readBinaryFile(filePath);
    if (data.length < 10) {
      throw new IOException("File too small or format incorrect");
    }

    ByteBuffer buffer = ByteBuffer.wrap(data).order(byteOrder);

    // Skip first 4 bytes of random number
    buffer.getInt();

    // Read XOR key
    byte xorKey = buffer.get();

    // Calculate encrypted text length
    int encryptedTextLength = data.length - 10; // Total length - header/footer 10 bytes

    // Decrypt text content
    byte[] encryptedText = new byte[encryptedTextLength];
    buffer.get(encryptedText);

    byte[] decryptedText = new byte[encryptedTextLength];
    for (int i = 0; i < encryptedTextLength; i++) {
      decryptedText[i] = (byte) (encryptedText[i] ^ xorKey);
    }

    return new String(decryptedText, StandardCharsets.UTF_8);
  }

  /** Read string from simplified binary file */
  public static String readStringFromRawBinary(String filePath, ByteOrder byteOrder)
      throws IOException {
    byte[] data = readBinaryFile(filePath);
    if (data.length < 4) {
      throw new IOException("File too small or format incorrect");
    }

    ByteBuffer buffer = ByteBuffer.wrap(data).order(byteOrder);
    int textLength = buffer.getInt();

    if (textLength > data.length - 4) {
      throw new IOException("Text length exceeds file range");
    }

    byte[] textBytes = new byte[textLength];
    buffer.get(textBytes);

    return new String(textBytes, StandardCharsets.UTF_8);
  }

  /** Write integer list to file */
  public static void writeIntegersToFile(
      String filePath, List<Integer> integers, boolean append, ByteOrder byteOrder)
      throws IOException {
    try (FileOutputStream fos = new FileOutputStream(filePath, append)) {
      for (int value : integers) {
        byte[] bytes = ByteBuffer.allocate(4).order(byteOrder).putInt(value).array();
        fos.write(bytes);
      }
    }
  }

  /** Write hexadecimal string to file */
  public static void writeHexStringToFile(String filePath, String hexString, boolean append)
      throws IOException {
    if (hexString == null || hexString.length() % 2 != 0) {
      throw new IllegalArgumentException("Hexadecimal string length must be even");
    }

    // Remove possible spaces
    hexString = hexString.replaceAll("\\s+", "");

    byte[] data = new byte[hexString.length() / 2];
    for (int i = 0; i < data.length; i++) {
      String byteStr = hexString.substring(i * 2, i * 2 + 2);
      data[i] = (byte) Integer.parseInt(byteStr, 16);
    }
    writeBytesToFile(filePath, data, append);
  }

  /** Write text to file (specified encoding) */
  public static void writeTextToFile(String filePath, String text, Charset charset, boolean append)
      throws IOException {
    byte[] data = text.getBytes(charset);
    writeBytesToFile(filePath, data, append);
  }

  public static void writeTextToFile(String filePath, String text, boolean append)
      throws IOException {
    writeTextToFile(filePath, text, StandardCharsets.UTF_8, append);
  }

  /** Write single integer (4 bytes) */
  public static void writeIntToFile(String filePath, int value, boolean append, ByteOrder byteOrder)
      throws IOException {
    byte[] data = ByteBuffer.allocate(4).order(byteOrder).putInt(value).array();
    writeBytesToFile(filePath, data, append);
  }

  /** Write single short integer (2 bytes) */
  public static void writeShortToFile(
      String filePath, short value, boolean append, ByteOrder byteOrder) throws IOException {
    byte[] data = ByteBuffer.allocate(2).order(byteOrder).putShort(value).array();
    writeBytesToFile(filePath, data, append);
  }

  /** Write single long integer (8 bytes) */
  public static void writeLongToFile(
      String filePath, long value, boolean append, ByteOrder byteOrder) throws IOException {
    byte[] data = ByteBuffer.allocate(8).order(byteOrder).putLong(value).array();
    writeBytesToFile(filePath, data, append);
  }

  /** Write single float (4 bytes) */
  public static void writeFloatToFile(
      String filePath, float value, boolean append, ByteOrder byteOrder) throws IOException {
    byte[] data = ByteBuffer.allocate(4).order(byteOrder).putFloat(value).array();
    writeBytesToFile(filePath, data, append);
  }

  /** Write single double precision float (8 bytes) */
  public static void writeDoubleToFile(
      String filePath, double value, boolean append, ByteOrder byteOrder) throws IOException {
    byte[] data = ByteBuffer.allocate(8).order(byteOrder).putDouble(value).array();
    writeBytesToFile(filePath, data, append);
  }

  /** Create empty file of specified size (filled with 0) */
  public static void createEmptyFile(String filePath, int size) throws IOException {
    byte[] emptyData = new byte[size];
    Arrays.fill(emptyData, (byte) 0);
    writeBytesToFile(filePath, emptyData, false);
  }

  /** Modify byte data at specified position */
  public static void modifyBytesAtPosition(String filePath, int position, byte[] newData)
      throws IOException {
    byte[] originalData = readBinaryFile(filePath);
    if (position + newData.length > originalData.length) {
      throw new IllegalArgumentException("Modification position exceeds file range");
    }

    System.arraycopy(newData, 0, originalData, position, newData.length);
    writeBytesToFile(filePath, originalData, false);
  }

  // ==================== File Parsing Methods ====================

  /** Convert byte array to hexadecimal string display */
  public static String bytesToHexString(byte[] data, int offset, int length) {
    if (data == null || data.length == 0) return "";

    int end = Math.min(offset + length, data.length);
    StringBuilder sb = new StringBuilder();

    for (int i = offset; i < end; i++) {
      sb.append(String.format("%02X ", data[i]));
      if ((i - offset + 1) % 16 == 0) {
        sb.append("\n");
      } else if ((i - offset + 1) % 8 == 0) {
        sb.append("  ");
      }
    }
    return sb.toString();
  }

  public static String bytesToHexString(byte[] data) {
    return bytesToHexString(data, 0, data.length);
  }

  /** Try to parse text content with different encodings */
  public static String decodeTextWithMultipleEncodings(byte[] data, int offset, int length) {
    if (data == null || data.length == 0) return "";

    int end = Math.min(offset + length, data.length);
    byte[] textData = Arrays.copyOfRange(data, offset, end);

    Charset[] charsets = {
      StandardCharsets.UTF_8,
      StandardCharsets.ISO_8859_1,
      Charset.forName("Windows-1252"),
      Charset.forName("GBK"),
      Charset.forName("GB2312"),
      StandardCharsets.UTF_16,
      StandardCharsets.UTF_16BE,
      StandardCharsets.UTF_16LE
    };

    StringBuilder result = new StringBuilder("Try multiple encoding parsing:\n\n");

    for (Charset charset : charsets) {
      try {
        String decoded = new String(textData, charset);
        // Filter out non-printable characters
        String cleanText = decoded.replaceAll("[^\\x20-\\x7E\\u4E00-\\u9FA5]", ".");
        result.append(String.format("%-15s: %s\n", charset.name(), cleanText));
      } catch (Exception e) {
        result.append(String.format("%-15s: Parse failed\n", charset.name()));
      }
    }

    return result.toString();
  }

  /** Parse integer data (4 bytes) */
  public static List<Integer> parseIntegers(
      byte[] data, int offset, int count, ByteOrder byteOrder) {
    List<Integer> integers = new ArrayList<>();
    int position = offset;

    for (int i = 0; i < count; i++) {
      if (position + 3 >= data.length) break;

      ByteBuffer buffer = ByteBuffer.wrap(data, position, 4);
      buffer.order(byteOrder);
      integers.add(buffer.getInt());
      position += 4;
    }

    return integers;
  }

  /** Parse short integer data (2 bytes) */
  public static List<Short> parseShorts(byte[] data, int offset, int count, ByteOrder byteOrder) {
    List<Short> shorts = new ArrayList<>();
    int position = offset;

    for (int i = 0; i < count; i++) {
      if (position + 1 >= data.length) break;

      ByteBuffer buffer = ByteBuffer.wrap(data, position, 2);
      buffer.order(byteOrder);
      shorts.add(buffer.getShort());
      position += 2;
    }

    return shorts;
  }

  /** Parse float data (4 bytes) */
  public static List<Float> parseFloats(byte[] data, int offset, int count, ByteOrder byteOrder) {
    List<Float> floats = new ArrayList<>();
    int position = offset;

    for (int i = 0; i < count; i++) {
      if (position + 3 >= data.length) break;

      ByteBuffer buffer = ByteBuffer.wrap(data, position, 4);
      buffer.order(byteOrder);
      floats.add(buffer.getFloat());
      position += 4;
    }

    return floats;
  }

  /** Parse double precision float data (8 bytes) */
  public static List<Double> parseDoubles(byte[] data, int offset, int count, ByteOrder byteOrder) {
    List<Double> doubles = new ArrayList<>();
    int position = offset;

    for (int i = 0; i < count; i++) {
      if (position + 7 >= data.length) break;

      ByteBuffer buffer = ByteBuffer.wrap(data, position, 8);
      buffer.order(byteOrder);
      doubles.add(buffer.getDouble());
      position += 8;
    }

    return doubles;
  }

  /** Find specific byte pattern */
  public static List<Integer> findPattern(byte[] data, byte[] pattern) {
    List<Integer> positions = new ArrayList<>();

    for (int i = 0; i <= data.length - pattern.length; i++) {
      boolean match = true;
      for (int j = 0; j < pattern.length; j++) {
        if (data[i + j] != pattern[j]) {
          match = false;
          break;
        }
      }
      if (match) {
        positions.add(i);
      }
    }

    return positions;
  }

  /** Analyze file header information */
  public static String analyzeFileHeader(byte[] data) {
    if (data.length < 8) return "File too small to analyze header";

    StringBuilder analysis = new StringBuilder("File header analysis:\n");
    analysis.append(String.format("File size: %d bytes\n", data.length));

    // Check common file signatures
    if (data.length >= 4) {
      // PNG
      if (data[0] == (byte) 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47) {
        analysis.append("File type: PNG image\n");
      }
      // JPEG
      else if (data[0] == (byte) 0xFF && data[1] == (byte) 0xD8 && data[2] == (byte) 0xFF) {
        analysis.append("File type: JPEG image\n");
      }
      // GIF
      else if (data[0] == 0x47 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x38) {
        analysis.append("File type: GIF image\n");
      }
      // PDF
      else if (data[0] == 0x25 && data[1] == 0x50 && data[2] == 0x44 && data[3] == 0x46) {
        analysis.append("File type: PDF document\n");
      }
      // ZIP (correct ZIP file header is 0x50 0x4B 0x03 0x04)
      else if (data[0] == 0x50 && data[1] == 0x4B && data[2] == 0x03 && data[3] == 0x04) {
        analysis.append("File type: ZIP compressed file\n");
      } else {
        analysis.append("File type: Unknown binary file\n");
      }
    }

    analysis.append(String.format("First 8 bytes: %s\n", bytesToHexString(data, 0, 8)));
    return analysis.toString();
  }

  /** Extract printable text content */
  public static String extractPrintableText(byte[] data, Charset charset) {
    StringBuilder text = new StringBuilder();
    StringBuilder currentLine = new StringBuilder();

    for (byte b : data) {
      char c = (char) (b & 0xFF);
      // Only keep printable ASCII characters and Chinese characters
      if ((c >= 32 && c <= 126) || (c >= 0x4E00 && c <= 0x9FA5)) {
        currentLine.append(c);
      } else if (currentLine.length() > 0) {
        // When encountering non-printable characters, wrap if current line has content
        if (currentLine.length() > 3) { // At least 3 characters considered valid text
          text.append(currentLine).append("\n");
        }
        currentLine.setLength(0);
      }
    }

    if (currentLine.length() > 3) {
      text.append(currentLine);
    }

    return text.toString();
  }

  /** Generate detailed file analysis report */
  public static String generateAnalysisReport(String filePath) throws IOException {
    byte[] data = readBinaryFile(filePath);
    StringBuilder report = new StringBuilder();

    report.append("=== Binary File Analysis Report ===\n\n");
    report.append(analyzeFileHeader(data)).append("\n");

    report.append("=== Hexadecimal View (first 256 bytes) ===\n");
    report.append(bytesToHexString(data, 0, Math.min(256, data.length))).append("\n\n");

    report.append("=== Text Content Parsing ===\n");
    report
        .append(decodeTextWithMultipleEncodings(data, 0, Math.min(100, data.length)))
        .append("\n");

    report.append("=== Printable Text Extraction ===\n");
    report.append(extractPrintableText(data, StandardCharsets.UTF_8)).append("\n\n");

    // Try to parse some common data types
    if (data.length >= 20) {
      report.append("=== Data Type Parsing Examples ===\n");

      // Parse first 5 integers (big endian)
      List<Integer> integers = parseIntegers(data, 0, 5, ByteOrder.BIG_ENDIAN);
      report.append("First 5 integers (big endian): ").append(integers).append("\n");

      // Parse first 5 integers (little endian)
      integers = parseIntegers(data, 0, 5, ByteOrder.LITTLE_ENDIAN);
      report.append("First 5 integers (little endian): ").append(integers).append("\n");

      // Parse first 5 floats (big endian)
      List<Float> floats = parseFloats(data, 0, 5, ByteOrder.BIG_ENDIAN);
      report.append("First 5 floats (big endian): ").append(floats).append("\n");
    }

    return report.toString();
  }
}
