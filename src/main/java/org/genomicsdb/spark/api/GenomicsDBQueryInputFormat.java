/*
 * The MIT License (MIT)
 * Copyright (c) 2020 Omics Data Automation, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.genomicsdb.spark.api;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.genomicsdb.model.GenomicsDBExportConfiguration;
import org.genomicsdb.reader.GenomicsDBQuery;
import org.genomicsdb.reader.GenomicsDBQuery.Interval;
import org.genomicsdb.reader.GenomicsDBQuery.VariantCall;
import org.genomicsdb.spark.*;

import java.io.IOException;
import java.util.List;

public class GenomicsDBQueryInputFormat
  extends AbstractGenomicsDBInputFormat<Interval, List<VariantCall>> implements Configurable {

  private Configuration configuration;
  private GenomicsDBInput<GenomicsDBInputSplit> input;

  Logger logger = Logger.getLogger(GenomicsDBQueryInputFormat.class);

  @Override
  public RecordReader<GenomicsDBQuery.Interval, List<VariantCall>>
    createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    // Need to amend query file being passed in based on inputSplit
    // so we'll create an appropriate protobuf object
    GenomicsDBExportConfiguration.ExportConfiguration exportConfiguration =
            createGenomicsDBExportConfiguration(inputSplit, taskAttemptContext);


    return new GenomicsDBQueryRecordReader(exportConfiguration, configuration.get(GenomicsDBConfiguration.LOADERJSON));
  }

  /**
   * default constructor
   */
  public GenomicsDBQueryInputFormat() {
    input = new GenomicsDBInput<>(null, null, null, 1, Long.MAX_VALUE, GenomicsDBInputSplit.class);
  }

  public GenomicsDBQueryInputFormat(GenomicsDBConfiguration conf) {
    input = new GenomicsDBInput<>(conf, null, null, 1, Long.MAX_VALUE, GenomicsDBInputSplit.class);
  }
}
