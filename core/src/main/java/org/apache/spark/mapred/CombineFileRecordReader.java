/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mapred;

import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapred.lib.CombineFileSplit;


/**
 * A generic RecordReader that can hand out different recordReaders
 * for each chunk in a {@link org.apache.hadoop.mapred.lib.CombineFileSplit}.
 * A CombineFileSplit can combine data chunks from multiple files.
 * This class allows using different RecordReaders for processing
 * these data chunks from different files.
 * @see org.apache.hadoop.mapred.lib.CombineFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineFileRecordReader<K, V> implements RecordReader<K, V> {

  protected CombineFileSplit split;
  protected JobConf jc;
  protected Reporter reporter;
  protected FileSystem fs;

  protected int idx;
  protected long progress;
  protected RecordReader<K, V> curReader;
  protected String currentPath;

  @Override
  public boolean next(K key, V value) throws IOException {

    while ((curReader == null) || !curReader.next(key, value)) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }
  
  public boolean next(K key, V value, CurrentPath path)  throws IOException {
    if(next(key, value)) {
      path.setPath(currentPath);
      return true;
    }
    return false;
  }

  public K createKey() {
    return curReader.createKey();
  }

  public V createValue() {
    return curReader.createValue();
  }

  /**
   * return the amount of data processed
   */
  public long getPos() throws IOException {
    return progress;
  }

  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }

  /**
   * return progress based on the amount of data processed so far.
   */
  public float getProgress() throws IOException {
    return Math.min(1.0f,  progress/(float)(split.getLength()));
  }
  private InputFormat<K, V> inputFormat;
  /**
   * A generic RecordReader that can hand out different recordReaders
   * for each chunk in the CombineFileSplit.
   */
  public CombineFileRecordReader(JobConf job, CombineFileSplit split,
                                 Reporter reporter,
                                 InputFormat<K, V> inputFormat)
    throws IOException {
    this.split = split;
    this.jc = job;
    this.reporter = reporter;
    this.idx = 0;
    this.curReader = null;
    this.progress = 0;
    this.inputFormat = inputFormat;
    initNextRecordReader();
  }

  /**
   * Get the record reader for the next chunk in this CombineFileSplit.
   */
  protected boolean initNextRecordReader() throws IOException {

    if (curReader != null) {
      curReader.close();
      curReader = null;
      if (idx > 0) {
        progress += split.getLength(idx-1);    // done processing so far
      }
    }

    // if all chunks have been processed, nothing more to do.
    if (idx == split.getNumPaths()) {
      return false;
    }

    reporter.progress();

    // get a record reader for the idx-th chunk
    try {
      curReader = new CombineFileRecordReaderWrapper((FileInputFormat)inputFormat, split, jc, reporter, idx){};
      currentPath = split.getPath(idx).toString();
      // setup some helper config variables.
      jc.set(JobContext.MAP_INPUT_FILE, currentPath);
      jc.setLong(JobContext.MAP_INPUT_START, split.getOffset(idx));
      jc.setLong(JobContext.MAP_INPUT_PATH, split.getLength(idx));
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    idx++;
    return true;
  }
}
