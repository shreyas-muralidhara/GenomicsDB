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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.*;

import org.genomicsdb.model.Coordinates;
import org.genomicsdb.model.GenomicsDBExportConfiguration;
import org.genomicsdb.model.GenomicsDBExportConfiguration.ExportConfiguration;
import org.genomicsdb.reader.GenomicsDBQuery;
import org.genomicsdb.reader.GenomicsDBQuery.Interval;
import org.genomicsdb.reader.GenomicsDBQuery.Pair;
import org.genomicsdb.reader.GenomicsDBQuery.VariantCall;
import org.genomicsdb.spark.GenomicsDBInputSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;


public class GenomicsDBQueryRecordReader extends RecordReader<Interval, List<VariantCall>> {
  ExportConfiguration exportConfiguration;
  String loaderJSONFile = "";

  List<Interval> intervals;
  Iterator<Interval> intervalIterator;
  Interval currentInterval;
  int numProcessedIntervals = 0;

  public GenomicsDBQueryRecordReader(final ExportConfiguration exportConfiguration, final String loaderJSONFile) {
    this.exportConfiguration = exportConfiguration;
    this.loaderJSONFile = loaderJSONFile;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    // Ignoring inputSplit, assuming exportConfiguration in the constructor is all we need
    initialize();
  }

  List<Pair> ToColumnRangePairs(List<Coordinates.GenomicsDBColumnOrInterval> intervalLists) {
    List<Pair> intervalPairs = new ArrayList<>();
    for (Coordinates.GenomicsDBColumnOrInterval interval : intervalLists) {
      assert (interval.getColumnInterval().hasTiledbColumnInterval());
      Coordinates.TileDBColumnInterval tileDBColumnInterval =
              interval.getColumnInterval().getTiledbColumnInterval();
      interval.getColumnInterval().getTiledbColumnInterval();
      assert (tileDBColumnInterval.hasBegin() && tileDBColumnInterval.hasEnd());
      intervalPairs.add(
              new Pair(tileDBColumnInterval.getBegin(), tileDBColumnInterval.getEnd()));
    }
    return intervalPairs;
  }

  List<Pair> ToRowRangePairs(List<GenomicsDBExportConfiguration.RowRange> rowRanges) {
    List<Pair> rangePairs = new ArrayList<>();
    for (GenomicsDBExportConfiguration.RowRange range : rowRanges) {
      assert (range.hasLow() && range.hasLow());
      rangePairs.add(new Pair(range.getLow(), range.getHigh()));
    }
    return rangePairs;
  }

  private void initialize() {
    if (!exportConfiguration.hasWorkspace()
            || !exportConfiguration.hasVidMapping()
            || !exportConfiguration.hasCallsetMapping()
            || !exportConfiguration.hasReferenceGenome()) {
      throw new RuntimeException("GenomicsDBExportConfiguration is incomplete. Add required configuration values and restart the operation");
    }

    GenomicsDBQuery query = new GenomicsDBQuery();

    long queryHandle = query.connectExportConfiguration(exportConfiguration, loaderJSONFile);

    intervals = query.queryVariantCalls(queryHandle, exportConfiguration.getArrayName(),
            ToColumnRangePairs(exportConfiguration.getQueryColumnRanges(0).getColumnOrIntervalListList()),
            ToRowRangePairs(exportConfiguration.getQueryRowRanges(0).getRangeListList()));

    query.disconnect(queryHandle);

    intervalIterator = intervals.iterator();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (intervalIterator.hasNext()) {
      currentInterval = intervalIterator.next();
      numProcessedIntervals++;
      return true;
    } else {
      currentInterval = null;
      return false;
    }
  }

  @Override
  public Interval getCurrentKey() throws IOException, InterruptedException {
    return currentInterval;
  }

  @Override
  public List<VariantCall> getCurrentValue() throws IOException, InterruptedException {
    return getCurrentKey().getCalls();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return numProcessedIntervals / intervals.size();
  }

  @Override
  public void close() throws IOException {
    //
  }
}

