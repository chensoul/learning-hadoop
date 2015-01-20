/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asp.tranlog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.asp.tranlog.ImportTsv.TsvParser.BadTsvLineException;

/**
 * Write table content out to files in hdfs.
 */
public class TsvImporterMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private static final Log LOG = LogFactory.getLog(TsvImporterMapper.class);

	/** Timestamp for all inserted rows */
	private long ts;

	/** Column seperator */
	private String separator;

	/** Should skip bad lines */
	private boolean skipBadLines;
	private Counter badLineCount;
	private int[] keyColIndex = null;// The column index that will be used to
										// compose to a row key like
										// aaaa+bbb+ccc
	private int[] keyColLen = null;
	private byte[] columnTypes = null;
	private char[] colDatetimeFormater = null;// For columns with type
												// 'datetime', the formater will
												// be saved in this array.

	private String charset;
	private String hbase_rowkey_separator;

	public final static SimpleDateFormat[] datetimeParsers = {
			new SimpleDateFormat("MMM dd yyyy hh:mm:ss:SSSaa",
					new java.util.Locale("en")),// Dec 7 2012 3:35:30:453PM
			new SimpleDateFormat("yyyyMMdd", new java.util.Locale("en")) };

	private ImportTsv.TsvParser parser;

	public long getTs() {
		return ts;
	}

	public boolean getSkipBadLines() {
		return skipBadLines;
	}

	public Counter getBadLineCount() {
		return badLineCount;
	}

	public void incrementBadLineCount(int count) {
		this.badLineCount.increment(count);
	}

	/**
	 * Handles initializing this class with objects specific to it (i.e., the
	 * parser). Common initialization that might be leveraged by a subsclass is
	 * done in <code>doSetup</code>. Hence a subclass may choose to override
	 * this method and call <code>doSetup</code> as well before handling it's
	 * own custom params.
	 * 
	 * @param context
	 */
	@Override
	protected void setup(Context context) {
		doSetup(context);

		Configuration conf = context.getConfiguration();

		charset = conf.get(ImportTsv.CHARSET_CONF_KEY);

		parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY),
				conf.getStrings(ImportTsv.KEYCOLUMNS_CONF_KEY), separator);
		keyColIndex = parser.getRowKeyColumnIndex();
		keyColLen = parser.getRowKeyColumnLen();
		if (keyColIndex == null) {
			throw new RuntimeException("No row key column specified");
		}
		columnTypes = parser.getColType();
		if (columnTypes != null) {
			colDatetimeFormater = new char[columnTypes.length];
			for (int i = 0; i < columnTypes.length; i++)
				colDatetimeFormater[i] = 0;
		}
	}

	/**
	 * Handles common parameter initialization that a subclass might want to
	 * leverage.
	 * 
	 * @param context
	 */
	protected void doSetup(Context context) {
		Configuration conf = context.getConfiguration();

		// If a custom separator has been used,
		// decode it back from Base64 encoding.
		separator = conf.get(ImportTsv.SEPARATOR_CONF_KEY);
		if (separator == null) {
			separator = ImportTsv.DEFAULT_SEPARATOR;
		} else {
			separator = new String(Base64.decode(separator));
		}

		hbase_rowkey_separator = conf.get(ImportTsv.SEPARATOR_CONF_ROWKEY);
		if (hbase_rowkey_separator == null
				|| hbase_rowkey_separator.trim().length() == 0) {
			hbase_rowkey_separator = "";
		} else {
			hbase_rowkey_separator = new String(
					Base64.decode(hbase_rowkey_separator));
		}

		ts = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY,
				System.currentTimeMillis());

		skipBadLines = context.getConfiguration().getBoolean(
				ImportTsv.SKIP_LINES_CONF_KEY, true);
		badLineCount = context.getCounter("ImportTsv", "Bad Lines");
	}

	/**
	 * To find a date parser from the datetimeParsers array
	 * 
	 * @return
	 */
	protected Date parseTimestamp(byte[] byteVal, int colIdx)
			throws ParseException {
		Date rtnDate = null;
		String dateString = Bytes.toString(byteVal);
		if (colDatetimeFormater != null && colDatetimeFormater.length > colIdx) {
			int fmtrIdx = colDatetimeFormater[colIdx];
			try {
				rtnDate = datetimeParsers[fmtrIdx].parse(dateString);
			} catch (java.text.ParseException e) {
			}

			if (rtnDate == null) {
				for (int i = 0; i < datetimeParsers.length; i++) {
					try {
						rtnDate = datetimeParsers[i].parse(dateString);
					} catch (java.text.ParseException e) {
					}
					if (rtnDate != null) {
						colDatetimeFormater[colIdx] = (char) i;
						break;
					}
				}
			}
		}
		if (rtnDate == null) {
			LOG.error("No supported data format found: " + dateString);
			throw new ParseException("Failed to parse date: " + dateString, 0);
		}
		return rtnDate;
	}

	/**
	 * Extract byte array for column specified by colIdx.
	 * 
	 * @param lineBytes
	 * @param parsed
	 * @param colIdx
	 * @return
	 */
	protected byte[] getInputColBytes(byte[] lineBytes,
			ImportTsv.TsvParser.ParsedLine parsed, int colIdx) {
		if (colIdx >= columnTypes.length)
			return null;
		int colOffset = parsed.getColumnOffset(colIdx);
		int colLen = parsed.getColumnLength(colIdx);
		byte[] colBytes = new byte[colLen];
		Bytes.putBytes(colBytes, 0, lineBytes, colOffset, colLen);
		return colBytes;
	}

	/**
	 * To create rowkey byte array, the rule is like this: row key can be
	 * composed by several columns change every columns values to String, if
	 * column type is date, change to long first if column values are "kv1  ",
	 * "kv2", "  kv3", ... then the row key string will be "kv1  +kv2+  kv3",
	 * that means the space char will be kept
	 * 
	 * @param lineBytes
	 * @param parsed
	 * @return
	 * @throws BadTsvLineException
	 */
	protected byte[] createRowkeyByteArray(byte[] lineBytes,
			ImportTsv.TsvParser.ParsedLine parsed) throws BadTsvLineException {
		try {

			byte[] colBytes = null;
			Date tmpDate = null;
			StringBuffer sb = new StringBuffer();

			for (int i = 0; i < keyColIndex.length; i++) {
				if (i > 0 && hbase_rowkey_separator.length() > 0)
					sb.append(hbase_rowkey_separator);
				colBytes = getInputColBytes(lineBytes, parsed, keyColIndex[i]);
				if (colBytes == null)
					throw new BadTsvLineException(
							"Failed to get column bytes for " + keyColIndex[i]);
				String rowCol;
				if (columnTypes[keyColIndex[i]] == ImportTsv.COL_TYPE_DATETIME) {
					tmpDate = parseTimestamp(colBytes, keyColIndex[i]);
					rowCol = Long.toString(tmpDate.getTime());
					sb.append(rowCol);
				} else if (columnTypes[keyColIndex[i]] == ImportTsv.COL_TYPE_STRING) {
					// String lineStr = new String(value.getBytes(), 0,
					// value.getLength(), "gb18030");
					// byte[] lineBytes = new Text(lineStr).getBytes();

					if (StringUtils.isEmpty(charset))
						charset = HConstants.UTF8_ENCODING;

					String lineStr = new String(colBytes, charset);
					colBytes = new Text(lineStr).getBytes();

					rowCol = Bytes.toString(colBytes);
					// if original string len < specified string len, then use
					// substring, else using space to right pad.
					if (keyColLen[i] != 0 && rowCol.length() > keyColLen[i])
						sb.append(rowCol.substring(0, keyColLen[i]));
					else
						sb.append(StringUtils.rightPad(rowCol, keyColLen[i]));
				} else if (columnTypes[keyColIndex[i]] == ImportTsv.COL_TYPE_INT) {
					int intVal = Integer.parseInt(Bytes.toString(colBytes));
					rowCol = Integer.toString(intVal);
					sb.append(StringUtils.leftPad(rowCol, keyColLen[i], '0'));
				} else if (columnTypes[keyColIndex[i]] == ImportTsv.COL_TYPE_DOUBLE) {
					double dbval = Double.parseDouble(Bytes.toString(colBytes));
					rowCol = Double.toString(dbval);
					sb.append(rowCol);
				} else if (columnTypes[keyColIndex[i]] == ImportTsv.COL_TYPE_LONG) {
					long longVal = Long.parseLong(Bytes.toString(colBytes));
					rowCol = Long.toString(longVal);
					sb.append(StringUtils.leftPad(rowCol, keyColLen[i], '0'));
				} else {
					rowCol = Bytes.toString(colBytes);
					// if original string len < specified string len, then use
					// substring, else using space to right pad.
					if (keyColLen[i] != 0 && rowCol.length() > keyColLen[i])
						sb.append(rowCol.substring(0, keyColLen[i]));
					else
						sb.append(StringUtils.rightPad(rowCol, keyColLen[i]));
				}
			}
			return sb.toString().getBytes();
		} catch (Exception e) {
			throw new BadTsvLineException(e.getMessage());
		}
	}

	/**
	 * 
	 * @param lineBytes
	 * @param parsed
	 * @param colIdx
	 * @return
	 */
	protected byte[] convertColBytes(byte[] lineBytes,
			ImportTsv.TsvParser.ParsedLine parsed, int colIdx)
			throws BadTsvLineException {
		byte[] rtn = null;
		byte[] srcBytes = getInputColBytes(lineBytes, parsed, colIdx);
		try {
			if (columnTypes[colIdx] == ImportTsv.COL_TYPE_DATETIME) {
				Date tmpDate = parseTimestamp(srcBytes, colIdx);
				;
				rtn = Bytes.toBytes(tmpDate.getTime());
			} else if (columnTypes[colIdx] == ImportTsv.COL_TYPE_INT) {
				int intVal = Integer.parseInt(Bytes.toString(srcBytes));
				rtn = Bytes.toBytes(intVal);
			} else if (columnTypes[colIdx] == ImportTsv.COL_TYPE_DOUBLE) {
				double dbval = Double.parseDouble(Bytes.toString(srcBytes));
				rtn = Bytes.toBytes(dbval);
			} else if (columnTypes[colIdx] == ImportTsv.COL_TYPE_LONG) {
				long longVal = Long.parseLong(Bytes.toString(srcBytes));
				rtn = Bytes.toBytes(longVal);
			} else {
				rtn = srcBytes;
			}
		} catch (Exception e) {
			throw new BadTsvLineException(e.getMessage());
		}
		return rtn;
	}

	/**
	 * Convert a line of TSV text into an HBase table row.
	 */
	@Override
	public void map(LongWritable offset, Text value, Context context)
			throws IOException {

		byte[] lineBytes = value.getBytes();

		// String lineStr = new String(value.getBytes(), 0, value.getLength(),
		// "gb18030");
		// byte[] lineBytes = new Text(lineStr).getBytes();

		int i = 0;
		try {
			ImportTsv.TsvParser.ParsedLine parsed = parser.parse(lineBytes,
					value.getLength());

			// ImportTsv.TsvParser.ParsedLine parsed = parser.parse(
			// lineBytes, Text.utf8Length(lineStr));

			byte[] rowKeyBytes = createRowkeyByteArray(lineBytes, parsed);
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(
					rowKeyBytes);

			Put put = new Put(rowKeyBytes);
			put.setWriteToWAL(false);

			for (i = 0; i < parsed.getColumnCount(); i++) {

				KeyValue kv = null;
				if (columnTypes[i] == ImportTsv.COL_TYPE_STRING) {
					kv = new KeyValue(rowKeyBytes, parser.getFamily(i),
							parser.getQualifier(i), 0,
							parser.getQualifier(i).length, ts,
							KeyValue.Type.Put, lineBytes,
							parsed.getColumnOffset(i),
							parsed.getColumnLength(i));
				} else {
					byte[] colBytes = convertColBytes(lineBytes, parsed, i);
					if (colBytes == null)
						throw new ImportTsv.TsvParser.BadTsvLineException(
								"Failed to get bytes for column " + i);
					kv = new KeyValue(rowKeyBytes, parser.getFamily(i),
							parser.getQualifier(i), ts, colBytes);
				}
				if (kv == null)
					throw new ImportTsv.TsvParser.BadTsvLineException(
							"Failed to get bytes for column " + i);
				put.add(kv);
			}
			context.write(rowKey, put);
		} catch (ImportTsv.TsvParser.BadTsvLineException badLine) {
			if (skipBadLines) {
				System.err.println("Bad line: "
						+ new String(lineBytes, "gb18030") + ":" + i + "\n");
				LOG.error("Bad line: " + new String(lineBytes, "gb18030") + ","
						+ i);
				incrementBadLineCount(1);
				return;
			} else {
				throw new IOException(badLine);
			}
		} catch (IllegalArgumentException e) {
			if (skipBadLines) {
				System.err.println("Bad line: "
						+ new String(lineBytes, "gb18030") + ":" + i + "\n");
				LOG.error("Bad line: " + new String(lineBytes, "gb18030") + ","
						+ i);
				incrementBadLineCount(1);
				return;
			} else {
				throw new IOException(e);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
