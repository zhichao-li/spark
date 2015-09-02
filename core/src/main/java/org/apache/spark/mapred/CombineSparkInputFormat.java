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

import java.io.IOException;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class CombineSparkInputFormat<K, V> extends CombineFileInputFormat<K, V> {

  private InputFormat<K, V> inputformat;

  public CombineSparkInputFormat(InputFormat<K, V> inputformat, int splitSize) {
    this.inputformat = inputformat;
    // If a maxSplitSize is specified, then blocks on the same node are
    // combined to form a single split. Blocks that are left over are
    // then combined with other blocks in the same rack.
    // If maxSplitSize is not specified(default 0), then blocks from the same rack
    // are combined in a single split; no attempt is made to create
    // node-local splits.
    super.setMaxSplitSize(splitSize);
  }

  @Override
  public void setMaxSplitSize(long size) {
    super.setMaxSplitSize(size);
  }
  
  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
     return new CombineFileRecordReader(job, (CombineFileSplit)split, Reporter.NULL, inputformat);
  }

}
