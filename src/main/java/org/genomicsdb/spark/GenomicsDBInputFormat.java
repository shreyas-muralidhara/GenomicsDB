/*
 * The MIT License (MIT)
 * Copyright (c) 2016-2017 Intel Corporation
 * Copyright (c) 2019-2020 Omics Data Automation, Inc.
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

import htsjdk.tribble.Feature;
import htsjdk.tribble.FeatureCodec;
import htsjdk.variant.bcf2.BCF2Codec;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;
import org.genomicsdb.model.GenomicsDBExportConfiguration;
import org.genomicsdb.reader.GenomicsDBFeatureReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

public class GenomicsDBInputFormat<VCONTEXT extends Feature, SOURCE>
  extends AbstractGenomicsDBInputFormat<String, VCONTEXT> implements Configurable {

  Logger logger = Logger.getLogger(GenomicsDBInputFormat.class);

  @Override
  public RecordReader<String, VCONTEXT>
    createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    GenomicsDBFeatureReader<VCONTEXT, SOURCE> featureReader;
    RecordReader<String, VCONTEXT> recordReader;

    // Need to amend query file being passed in based on inputSplit
    // so we'll create an appropriate protobuf object
    GenomicsDBExportConfiguration.ExportConfiguration exportConfiguration = createGenomicsDBExportConfiguration(inputSplit, taskAttemptContext);

      //GenomicsDBExportConfiguration.ExportConfiguration.Builder exportConfigurationBuilder = GenomicsDBExportConfiguration.ExportConfiguration.newBuilder();
      //JsonFormat.merge(queryJson, exportConfigurationBuilder);
      //GenomicsDBExportConfiguration.ExportConfiguration exportConfiguration = exportConfigurationBuilder
      //.setWorkspace("").setReferenceGenome("").build();

      //featureReader = new GenomicsDBFeatureReader<>(exportConfiguration,
      //        (FeatureCodec<VCONTEXT,SOURCE>) new BCF2Codec(), Optional.of(loaderJson));
      featureReader = getGenomicsDBFeatureReader(exportConfiguration, configuration.get(GenomicsDBConfiguration.LOADERJSON));
      return new GenomicsDBRecordReader<>(featureReader);
  }

  // create helper function so we can limit scope of SuppressWarnings
  @SuppressWarnings("unchecked")
  private GenomicsDBFeatureReader<VCONTEXT,SOURCE> getGenomicsDBFeatureReader(
          GenomicsDBExportConfiguration.ExportConfiguration pb, String loaderJson)
          throws IOException {
    // TODO: Should we move to using VCFCodec instead of BCF2Codec?
    return new GenomicsDBFeatureReader<>(pb, 
            (FeatureCodec<VCONTEXT,SOURCE>) new BCF2Codec(), Optional.of(loaderJson));
  }

  /**
   * default constructor
   */
  public GenomicsDBInputFormat() {
    input = new GenomicsDBInput<>(null, null, null, 1, Long.MAX_VALUE, GenomicsDBInputSplit.class);
  }

  public GenomicsDBInputFormat(GenomicsDBConfiguration conf) {
    input = new GenomicsDBInput<>(conf, null, null, 1, Long.MAX_VALUE, GenomicsDBInputSplit.class);
  }

  /**
   * Set the loader JSON file path
   *
   * @param jsonFile  Full qualified path of the loader JSON file
   * @return  Returns the same object for forward function calls
   */
  public GenomicsDBInputFormat<VCONTEXT, SOURCE> setLoaderJsonFile(String jsonFile) {
    input.getGenomicsDBConfiguration().setLoaderJsonFile(jsonFile);
    return this;
  }

  /**
   * Set the query JSON file path
   * @param jsonFile  Full qualified path of the query JSON file
   * @return  Returns the same object for forward function calls
   */
  public GenomicsDBInputFormat<VCONTEXT, SOURCE> setQueryJsonFile(String jsonFile) {
    input.getGenomicsDBConfiguration().setQueryJsonFile(jsonFile);
    return this;
  }

  /**
   * Set the host file path
   * @param hostFile  Full qualified path of the hosts file
   * @return  Returns the same object for forward function calls
   * @throws FileNotFoundException thrown if the hosts file is not found
   */
  public GenomicsDBInputFormat<VCONTEXT, SOURCE> setHostFile(String hostFile)
      throws FileNotFoundException {
    input.getGenomicsDBConfiguration().setHostFile(hostFile);
    return this;
  }
}
