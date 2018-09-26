/*
 * The MIT License (MIT)
 * Copyright (c) 2016-2017 Intel Corporation
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

package com.intel.genomicsdb.spark;

import com.intel.genomicsdb.reader.GenomicsDBFeatureReader;
import htsjdk.tribble.Feature;
import htsjdk.tribble.FeatureCodec;
import htsjdk.variant.bcf2.BCF2Codec;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GenomicsDBInputFormat<VCONTEXT extends Feature, SOURCE>
  extends InputFormat<String, VCONTEXT> implements Configurable {

  private GenomicsDBConfiguration genomicsDBConfiguration;
  private Configuration configuration;

  Logger logger = Logger.getLogger(GenomicsDBInputFormat.class);

  /**
   * When this function is called, it is already assumed that configuration
   * object is set
   *
   * @param jobContext  Hadoop Job context passed from newAPIHadoopRDD
   *                    defined in SparkContext
   * @return  Returns a list of input splits
   * @throws FileNotFoundException  Thrown if creaing configuration object fails
   */
  public List<InputSplit> getSplits(JobContext jobContext) throws FileNotFoundException {

    // TODO: maybe parameterize min & max?
    long minSize = 1;
    long maxSize = Long.MAX_VALUE;

    genomicsDBConfiguration = new GenomicsDBConfiguration(configuration);
    genomicsDBConfiguration.setLoaderJsonFile(
      configuration.get(GenomicsDBConfiguration.LOADERJSON));
    genomicsDBConfiguration.setQueryJsonFile(
      configuration.get(GenomicsDBConfiguration.QUERYJSON));
    genomicsDBConfiguration.setHostFile(
      configuration.get(GenomicsDBConfiguration.MPIHOSTFILE));

    try {
      genomicsDBConfiguration.populateListFromJson(GenomicsDBConfiguration.LOADERJSON);
      genomicsDBConfiguration.populateListFromJson(GenomicsDBConfiguration.QUERYJSON);
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    }
    catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    catch (ParseException e) {
      e.printStackTrace();
      return null;
    }

    ArrayList<GenomicsDBPartitionInfo> partitionsList = genomicsDBConfiguration.getPartitions();
    ArrayList<GenomicsDBQueryInfo> queryRangeList = genomicsDBConfiguration.getQueryRanges();

    long goalBlockSize = Math.max(minSize, 
		           Math.min(genomicsDBConfiguration.getQueryBlockSize(), maxSize));

    ArrayList<InputSplit> inputSplits = new ArrayList<InputSplit>();
    // For now, assuming that each of partitions and queryRange are sorted
    // by start position, and that query ranges don't overlap.
    // TODO: not sure anything in GenomicsDB enforces the above assumption....
    int pIndex = 0;
    int qIndex = 0;
    GenomicsDBPartitionInfo partition = null;
    if (!partitionsList.isEmpty())
      partition = partitionsList.get(pIndex);

    // if workspace contains hdfs://, or s3:// or gc:// they're hdfs compliant and we support it
    if (partition != null && !(partition.getWorkspace().contains("s3://") ||
		partition.getWorkspace().contains("hdfs://") ||  
		partition.getWorkspace().contains("gs://"))) {
      List<String> hosts = genomicsDBConfiguration.getHosts();
      for (int i=0; i<hosts.size(); i++) {
        inputSplits.add(new GenomicsDBInputSplit(hosts.get(i)));
      }
    }
    else if (partition != null) {
      // create a temporary arraylist that we'll use if we glom queries
      ArrayList<GenomicsDBQueryInfo> glomQuerys = new ArrayList<GenomicsDBQueryInfo>();
      while (qIndex < queryRangeList.size() && partition != null) {
        GenomicsDBQueryInfo queryRange = queryRangeList.get(qIndex);
  
        // advance partition index if needed
        // i.e., advance till we find the partition that contains the query begin position
        while ((pIndex + 1) < partitionsList.size() && 
                partitionsList.get(pIndex+1).getBeginPosition() <= queryRange.getBeginPosition()) {
          // add glommed queries to inputsplit using previous parition since
          // we're moving on to new partitions
          if (!glomQuerys.isEmpty()) {
            inputSplits.add(new GenomicsDBInputSplit(partition, glomQuerys));
            glomQuerys.clear();
          }
          pIndex++;
  	  partition = partitionsList.get(pIndex);
        }
  
        long queryBlockSize = queryRange.getEndPosition() - queryRange.getBeginPosition() + 1;
        if (queryBlockSize < goalBlockSize) {
          // create glommed inputsplit
  	  if ((pIndex + 1) < partitionsList.size() && 
               queryRange.getEndPosition() >= partitionsList.get(pIndex+1).getBeginPosition()) {
            // if current query spans two partitions then add to previous inputsplit as well
            if (queryRange.getBeginPosition() < partitionsList.get(pIndex+1).getBeginPosition()) {
              glomQuerys.add(queryRange);
            }
            inputSplits.add(new GenomicsDBInputSplit(partition, glomQuerys));
            glomQuerys.clear();
  	    // if this queryBlock spans multiple partitions, need to add those as splits as well
  	    // can use the same ArrayList of queries since each inputsplit will only care
  	    // about the section that is relevant to its partition
            glomQuerys.add(queryRange);
    	    while ((pIndex + 1) < partitionsList.size() &&
                    queryRange.getEndPosition() >= partitionsList.get(pIndex+1).getBeginPosition()) {
    	      pIndex++;
              partition = partitionsList.get(pIndex);
    	      inputSplits.add(new GenomicsDBInputSplit(partition, glomQuerys));
    	    }
            glomQuerys.clear();
          }
          else {
            // could do more smarts here to not glom?
            // maybe keep a running tally of the glommed range and compare that against
            // goalBlockSize. however, that really only matters in the case where
            // users choose to query a very large number of snps individually instead
            // of querying the entire region...probably not worth worrying about that
            glomQuerys.add(queryRange);
          }
        }
        else {
          if (!glomQuerys.isEmpty()) {
            inputSplits.add(new GenomicsDBInputSplit(partition, glomQuerys));
            glomQuerys.clear();
          }
          // bigger than goalBlockSize, so break up into "query chunks"
  
  	  long queryBlockStart = queryRange.getBeginPosition();
	  long queryBlockMargin = genomicsDBConfiguration.getQueryBlockSizeMargin();
  	  while (queryBlockStart < queryRange.getEndPosition()) {
            long blockSize = (queryBlockSize > (goalBlockSize+queryBlockMargin)) ? goalBlockSize : queryBlockSize;
  	    GenomicsDBQueryInfo queryBlock = new GenomicsDBQueryInfo(queryBlockStart, queryBlockStart + blockSize - 1);
            glomQuerys.add(queryBlock);
  	    inputSplits.add(new GenomicsDBInputSplit(partition, glomQuerys));
  
  	    // if this queryBlock spans multiple partitions, need to add those as splits as well
  	    while ((pIndex + 1) < partitionsList.size() &&
                    queryBlockStart + blockSize - 1 >= partitionsList.get(pIndex+1).getBeginPosition()) {
  	      pIndex++;
              partition = partitionsList.get(pIndex);
  	      inputSplits.add(new GenomicsDBInputSplit(partition, glomQuerys));
  	    }
            glomQuerys.clear();
  	    queryBlockStart += blockSize;
  	    queryBlockSize -= blockSize;
  	  }
        }
        qIndex++;
      }
      // if we still have glommed queries that haven't been assigned to
      // an inputsplit, they go with the final partition
      if(!glomQuerys.isEmpty()) {
        inputSplits.add(new GenomicsDBInputSplit(partition, glomQuerys));
      }
    }
    return inputSplits;
  }

  /**
   * Creates string of query ranges for query json
   *
   * @param qList ArrayList of GenomicsDBQueryInfo
   * @return Returns string of query ranges
   */
  String getQueryRangesString(ArrayList<GenomicsDBQueryInfo> qList) {
    String queryRanges = "[[";
    for (GenomicsDBQueryInfo q: qList) {
      if (queryRanges != "[[") {
        queryRanges += ",";
      }
      if (q.getBeginPosition() == q.getEndPosition()) {
        queryRanges += q.getBeginPosition();
      } else {
        queryRanges += "[" + q.getBeginPosition() + "," + q.getEndPosition() + "]";
      }
    }
    queryRanges += "]]";
    return queryRanges;
  }
  /**
   * Creates tmp query file based on inputSplit and existing query file
   *
   * @param queryJson Existing query json file
   * @param inputSplit used to populate workspace, array and query_column_ranges
   * @return  Returns path to temporary query file
   * @throws FileNotFoundException  Thrown if queryJson file isn't found
   * @throws IOException  Thrown if other IO exception while handling file operations
   * @throws ParseException  Thrown if JSON parsing fails
   */
  String createTmpQueryFile(String queryJson, GenomicsDBInputSplit inputSplit) 
		  throws FileNotFoundException, IOException, ParseException {
    String indentString = "    ";
    String amendedQuery = "{\n";
    amendedQuery += indentString + "\"workspace\": \""+inputSplit.getPartitionInfo().getWorkspace()+"\",\n";
    amendedQuery += indentString + "\"array\": \""+inputSplit.getPartitionInfo().getArrayName()+"\",\n";
    amendedQuery += indentString + "\"query_column_ranges\": " + getQueryRangesString(inputSplit.getQueryInfoList());

    try {

      JSONParser parser = new JSONParser();
      FileReader queryJsonReader = new FileReader(queryJson);
      JSONObject obj = null;
      try {
        obj = (JSONObject)parser.parse(queryJsonReader);
      }
      catch(ParseException | IOException e) {
        queryJsonReader.close();
        throw e;
      }
  
      if (obj.containsKey("query_row_ranges")) {
        amendedQuery += ",\n" + indentString + "\"query_row_ranges\": "+obj.get("query_row_ranges").toString()+"";
      }
      if (obj.containsKey("vid_mapping_file")) {
        amendedQuery += ",\n" + indentString + "\"vid_mapping_file\": \""+obj.get("vid_mapping_file").toString()+"\"";
      }
      if (obj.containsKey("callset_mapping_file")) {
        amendedQuery += ",\n" + indentString + "\"callset_mapping_file\": \""+obj.get("callset_mapping_file").toString()+"\"";
      }
      if (obj.containsKey("vcf_header_filename")) {
        amendedQuery += ",\n" + indentString + "\"vcf_header_filename\": "+obj.get("vcf_header_filename").toString();
      }
      if (obj.containsKey("query_attributes")) {
        amendedQuery += ",\n" + indentString + "\"query_attributes\": "+obj.get("query_attributes").toString();
      }
      if (obj.containsKey("reference_genome")) {
        amendedQuery += ",\n" + indentString + "\"reference_genome\": \""+obj.get("reference_genome").toString()+"\"";
      }
      if (obj.containsKey("segment_size")) {
        amendedQuery += ",\n" + indentString + "\"segment_size\": "+obj.get("segment_size").toString();
      }
      if (obj.containsKey("produce_GT_field")) {
        amendedQuery += ",\n" + indentString + "\"produce_GT_field\": "+obj.get("produce_GT_field").toString();
      }
      if (obj.containsKey("produce_FILTER_field")) {
        amendedQuery += ",\n" + indentString + "\"produce_FILTER_field\": "+obj.get("produce_FILTER_field").toString();
      }
      amendedQuery += "\n}\n";
      queryJsonReader.close();
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    }
    catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    catch (ParseException e) {
      e.printStackTrace();
      return null;
    }
    
    File tmpQueryFile = File.createTempFile("queryJson", ".json");
    tmpQueryFile.deleteOnExit();
    FileWriter fptr = new FileWriter(tmpQueryFile);
    try {
        fptr.write(amendedQuery);
    }
    catch(IOException e) {
        fptr.close();
        throw new IOException(e);
    }
    fptr.close();
    return tmpQueryFile.getAbsolutePath();
  }

  public RecordReader<String, VCONTEXT>
    createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    String loaderJson;
    String queryJson;

    GenomicsDBFeatureReader<VCONTEXT, SOURCE> featureReader;
    GenomicsDBRecordReader<VCONTEXT, SOURCE> recordReader;
    GenomicsDBInputSplit gSplit = (GenomicsDBInputSplit)inputSplit;

    if (taskAttemptContext != null) {
      Configuration configuration = taskAttemptContext.getConfiguration();
      loaderJson = configuration.get(GenomicsDBConfiguration.LOADERJSON);
      queryJson = configuration.get(GenomicsDBConfiguration.QUERYJSON);
    } else {
      // If control comes here, means this method is called from
      // GenomicsDBRDD. Hence, the configuration object must be
      // set by setConf method, else this will lead to
      // NullPointerException
      assert(configuration!=null);
      loaderJson = configuration.get(GenomicsDBConfiguration.LOADERJSON);
      queryJson = configuration.get(GenomicsDBConfiguration.QUERYJSON);
    }

    // Need to amend query file being passed in based on inputSplit
    // so we'll create a temporary query file
    // only do this IF we are using hdfs compliant data store i.e., 
    // getPartitionInfo is not null
    String amendedQuery = queryJson;
    if (gSplit.getPartitionInfo() != null) {
      try {
        amendedQuery = createTmpQueryFile(queryJson, gSplit);
      }
      catch (ParseException e) {
        e.printStackTrace();
        return null;
      }
    }
    //GenomicsDBExportConfiguration.ExportConfiguration.Builder exportConfigurationBuilder = GenomicsDBExportConfiguration.ExportConfiguration.newBuilder();
    //JsonFormat.merge(queryJson, exportConfigurationBuilder);
    //GenomicsDBExportConfiguration.ExportConfiguration exportConfiguration = exportConfigurationBuilder
            //.setWorkspace("").setReferenceGenome("").build();

    featureReader = new GenomicsDBFeatureReader<>(amendedQuery,
            (FeatureCodec<VCONTEXT, SOURCE>) new BCF2Codec(), Optional.of(loaderJson));
    recordReader = new GenomicsDBRecordReader<>(featureReader);
    return recordReader;
  }

  /**
   * default constructor
   */
  public GenomicsDBInputFormat() {
  }

  public GenomicsDBInputFormat(GenomicsDBConfiguration conf) {
    genomicsDBConfiguration = conf;
  }

  /**
   * Set the loader JSON file path
   *
   * @param jsonFile  Full qualified path of the loader JSON file
   * @return  Returns the same object for forward function calls
   */
  public GenomicsDBInputFormat<VCONTEXT, SOURCE> setLoaderJsonFile(String jsonFile) {
    genomicsDBConfiguration.setLoaderJsonFile(jsonFile);
    return this;
  }

  /**
   * Set the query JSON file path
   * @param jsonFile  Full qualified path of the query JSON file
   * @return  Returns the same object for forward function calls
   */
  public GenomicsDBInputFormat<VCONTEXT, SOURCE> setQueryJsonFile(String jsonFile) {
    genomicsDBConfiguration.setQueryJsonFile(jsonFile);
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
    genomicsDBConfiguration.setHostFile(hostFile);
    return this;
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
