package com.embracesource.jmeter.hbase;

public class Strings {
  public static String repeat(char ch, int repeat) {
    char[] buf = new char[repeat];
    for (int i = repeat - 1; i >= 0; i--) {
      buf[i] = ch;
    }
    return new String(buf);
  }
}