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

package org.genomicsdb.spark;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.genomicsdb.model.GenomicsDBExportConfiguration;
import org.genomicsdb.model.GenomicsDBExportConfiguration.ExportConfiguration;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

public abstract class AbstractGenomicsDBInputFormat<K, V> extends InputFormat<K, V> implements Configurable {
  protected Configuration configuration;
  protected GenomicsDBInput<GenomicsDBInputSplit> input;

  /**
   * When this function is called, it is already assumed that configuration
   * object is set
   *
   * @param jobContext  Hadoop Job context passed from newAPIHadoopRDD
   *                    defined in SparkContext
   * @return  Returns a list of input splits
   * @throws  IOException if creating configuration object fails
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    GenomicsDBConfiguration genomicsDBConfiguration = new GenomicsDBConfiguration(configuration);
    genomicsDBConfiguration.setLoaderJsonFile(
            configuration.get(GenomicsDBConfiguration.LOADERJSON));
    if (configuration.get(GenomicsDBConfiguration.QUERYPB) != null) {
      genomicsDBConfiguration.setQueryJsonFile(
              configuration.get(GenomicsDBConfiguration.QUERYPB));
    }
    else {
      genomicsDBConfiguration.setQueryJsonFile(
              configuration.get(GenomicsDBConfiguration.QUERYJSON));
    }
    if (configuration.get(GenomicsDBConfiguration.MPIHOSTFILE) != null) {
      genomicsDBConfiguration.setHostFile(
              configuration.get(GenomicsDBConfiguration.MPIHOSTFILE));
    }

    input.setGenomicsDBConfiguration(genomicsDBConfiguration);

    return (List) input.divideInput();
  }

  protected GenomicsDBExportConfiguration.ExportConfiguration createGenomicsDBExportConfiguration(
          InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    String query;

    GenomicsDBInputSplit gSplit = (GenomicsDBInputSplit)inputSplit;

    boolean isPB;
    if (taskAttemptContext != null) {
      Configuration configuration = taskAttemptContext.getConfiguration();
      if (configuration.get(GenomicsDBConfiguration.QUERYPB) != null) {
        query = configuration.get(GenomicsDBConfiguration.QUERYPB);
        isPB = true;
      }
      else {
        query = configuration.get(GenomicsDBConfiguration.QUERYJSON);
        isPB = false;
      }
    } else {
      // If control comes here, means this method is called from
      // GenomicsDBRDD. Hence, the configuration object must be
      // set by setConf method, else this will lead to
      // NullPointerException
      assert(configuration!=null);
      if (configuration.get(GenomicsDBConfiguration.QUERYPB) != null) {
        query = configuration.get(GenomicsDBConfiguration.QUERYPB);
        isPB = true;
      }
      else {
        query = configuration.get(GenomicsDBConfiguration.QUERYJSON);
        isPB = false;
      }
    }

    // Need to amend query file being passed in based on inputSplit
    // so we'll create an appropriate protobuf object
    ExportConfiguration exportConfiguration = null;
    try {
      exportConfiguration = GenomicsDBInput.createTargetExportConfigurationPB(query,
              gSplit.getPartitionInfo(), gSplit.getQueryInfoList(), isPB);
    }
    catch (ParseException e) {
      e.printStackTrace();
    } finally{
      return exportConfiguration;
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

}
