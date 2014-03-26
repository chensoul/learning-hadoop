package com.embracesource.jmeter.hbase;
import java.util.Random;
import java.util.UUID;

public class SequenceKey {
  static String[] str = new String[] { "B1C", "B1F", "B1R", "B2C", "B2F",
      "B2R", "C1C", "C1F", "C1R", "F1C", "F1F", "F1R", "F2C", "F2F", "F2R",
      "G1C", "G1F", "G1R", "H1C", "H1F", "H1R", "H2C", "H2F", "H2R", "H3C",
      "H3F", "H3R", "J1C", "J1F", "J1R", "K1C", "K1F", "K1R", "K2C", "K2F",
      "K2R", "M1C", "M1F", "M1R", "N1C", "N1F", "N1R", "N2C", "N2F", "N2R",
      "N3C", "N3F", "N3R", "O1C", "O1F", "O1R", "P1C", "P1F", "P1R", "P2C",
      "P2F", "P2R", "P3C", "P3F", "P3R", "Q1C", "Q1F", "Q1R", "Q2C", "Q2F",
      "Q2R", "Q6C", "Q6F", "Q6R", "Q7C", "Q7F", "Q7R", "R1C", "R1F", "R1R",
      "T1C", "T1F", "T1R", "T2C", "T2F", "T2R", "V1C", "V1F", "V1R", "W1C",
      "W1F", "W1R", "Y1C", "Y1F", "Y1R", "Z1C", "Z1F", "Z1R" };

  public static String getsequenceKey() {
    return str[new Random().nextInt(93)] + UUID.randomUUID();
  }

  public static void main(String[] args) {
    System.out.println(str.length);
    for (int i = 0; i < 1000; i++) {
      System.out.println(getsequenceKey());

    }
  }
}