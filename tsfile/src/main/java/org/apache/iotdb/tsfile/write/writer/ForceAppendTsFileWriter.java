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
package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * ForceAppendTsFileWriter opens a COMPLETE TsFile, reads and truncate its metadata to support
 * appending new data.
 */
public class ForceAppendTsFileWriter extends TsFileIOWriter {

  private final long truncatePosition;
  private static final Logger logger = LoggerFactory.getLogger(ForceAppendTsFileWriter.class);

  public ForceAppendTsFileWriter(File file) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{} writer is opened.", file.getName());
    }
    this.tsFileOutput =
        FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath(), true);
    this.indexFileOutput =
        FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath() + ".index", true);
    this.file = file;

    // file doesn't exist
    if (file.length() == 0 || !file.exists()) {
      throw new TsFileNotCompleteException("File " + file.getPath() + " is not a complete TsFile");
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), true)) {

      // this tsfile is not complete
      if (!reader.isComplete()) {
        throw new TsFileNotCompleteException(
            "File " + file.getPath() + " is not a complete TsFile");
      }
      // truncate metadata and marker
      truncatePosition = reader.getFileMetadataPos();
      canWrite = true;

      for (Map.Entry<String, List<ChunkMetadata>> entry :
          reader.readAllChunkMetadata().entrySet()) {
        ChunkGroupMetadata chunkGroupMetadata =
            new ChunkGroupMetadata(entry.getKey(), entry.getValue());
        chunkGroupMetadataList.add(chunkGroupMetadata);
      }
    }
  }

  public void doTruncate() throws IOException {
    tsFileOutput.truncate(truncatePosition);
    indexFileOutput.truncate(0);
  }

  public long getTruncatePosition() {
    return truncatePosition;
  }
}
