/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.writelog.recover;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import javafx.collections.transformation.SortedList;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogReplayer finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the
 * WALs from the logNode and redoes them into a given MemTable and ModificationFile.
 */
public class LogReplayer {

  private Logger logger = LoggerFactory.getLogger(LogReplayer.class);
  private String logNodePrefix;
  private String insertFilePath;
  private ModificationFile modFile;
  private VersionController versionController;
  private TsFileResource currentTsFileResource;
  private IMemTable recoverMemTable;

  // only unsequence file tolerates duplicated data
  private boolean sequence;

  private Map<String, Long> tempStartTimeMap = new HashMap<>();
  private Map<String, Long> tempEndTimeMap = new HashMap<>();

  public LogReplayer(String logNodePrefix, String insertFilePath, ModificationFile modFile,
      VersionController versionController, TsFileResource currentTsFileResource,
      IMemTable memTable, boolean sequence) {
    this.logNodePrefix = logNodePrefix;
//    this.insertFilePath = "/Users/timbo/var/opt/bznea/docker/iotdb/data/wal/root.bznea-1663790326623-14-0.tsfile";
    this.insertFilePath = "/Users/timbo/var/opt/bznea/docker/iotdb/data/wal/root.bznea-1664123421176-16-0.tsfile";
    this.modFile = modFile;
    this.versionController = versionController;
    this.currentTsFileResource = currentTsFileResource;
    this.recoverMemTable = memTable;
    this.sequence = sequence;
  }

  /**
   * finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the WALs from
   * the logNode and redoes them into a given MemTable and ModificationFile.
   */
  public void replayLogs() {
    Map<String,Map<Long,Map<String,WalHelper>>> allData = new HashMap<>();
    File[] logFiles = SystemFileFactory.INSTANCE.getFile(this.insertFilePath).listFiles();
    Arrays.sort(logFiles,
            Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace("wal", ""))));
    ILogReader logReader = new MultiFileLogReader(logFiles);
    long itemCounter = 0;
    try {
      while (logReader.hasNext()) {
        try {
          PhysicalPlan plan = logReader.next();
          if (plan instanceof InsertPlan) {
            if (plan instanceof InsertRowPlan) {
              if(itemCounter>4000000) {
                String device = ((InsertPlan) plan).getDeviceId().getDevice();
                String[] measurements = ((InsertPlan) plan).getMeasurements();
                TSDataType[] dataTypes = ((InsertPlan) plan).getDataTypes();
                long timestamp = ((InsertRowPlan) plan).getTime();
                Object[] values = ((InsertRowPlan) plan).getValues();
                if (!allData.containsKey(device)) {
                  allData.put(device, new HashMap<>());
                }
                if (!allData.get(device).containsKey(timestamp)) {
                  allData.get(device).put(timestamp, new HashMap<>());
                }
                allData.get(device).get(timestamp).put(measurements[0],new WalHelper(dataTypes[0],values[0]));
              }


//              if(itemCounter>4000000){
//                writeCsvs(allData,"/development/bznea/queries");
//                return;
//              }
              //assume array has always only one position


//              System.out.println("====================");
//              System.out.println("Device: " + device);
//              System.out.println("Timestamp: " + timestamp);
//              System.out.println("Measurements: " + Arrays.toString(measurements));
//              System.out.println("DataTypes: " + Arrays.toString(dataTypes));
//              System.out.println("Values: " + Arrays.toString(values));
              System.out.println("ItemCounter: " + itemCounter);
              itemCounter ++;
            } else {
              System.out.println("bäh");
            }
//            replayInsert((InsertPlan) plan);
          } else if (plan instanceof DeletePlan) {
//            replayDelete((DeletePlan) plan);
            System.out.println("Aha");
          } else if (plan instanceof UpdatePlan) {
//            replayUpdate((UpdatePlan) plan);
            System.out.println("Ehe");
          }
        } catch (Exception e) {
          logger.error("recover wal of {} failed", insertFilePath, e);
        }
      }
    } catch (IOException e) {
      logger.error("meet error when redo wal of {}", insertFilePath, e);
    } finally {
      logReader.close();
      writeCsvs(allData,"/development/bznea/queries");
    }
  }

  private void replayDelete(DeletePlan deletePlan) throws IOException, MetadataException {
    List<PartialPath> paths = deletePlan.getPaths();
    for (PartialPath path : paths) {
      for (PartialPath device : IoTDB.metaManager.getDevices(path.getDevicePath())) {
        recoverMemTable
            .delete(path, device, deletePlan.getDeleteStartTime(),
                deletePlan.getDeleteEndTime());
      }
      modFile
          .write(
              new Deletion(path, versionController.nextVersion(), deletePlan.getDeleteStartTime(),
                  deletePlan.getDeleteEndTime()));
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void replayInsert(InsertPlan plan) throws WriteProcessException, QueryProcessException {
    if (currentTsFileResource != null) {
      long minTime, maxTime;
      if (plan instanceof InsertRowPlan) {
        minTime = ((InsertRowPlan) plan).getTime();
        maxTime = ((InsertRowPlan) plan).getTime();
      } else {
        minTime = ((InsertTabletPlan) plan).getMinTime();
        maxTime = ((InsertTabletPlan) plan).getMaxTime();
      }
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      long lastEndTime = currentTsFileResource.getEndTime(plan.getDeviceId().getFullPath());
      if (lastEndTime != Long.MIN_VALUE && lastEndTime >= minTime &&
          sequence) {
        return;
      }
      Long startTime = tempStartTimeMap.get(plan.getDeviceId().getFullPath());
      if (startTime == null || startTime > minTime) {
        tempStartTimeMap.put(plan.getDeviceId().getFullPath(), minTime);
      }
      Long endTime = tempEndTimeMap.get(plan.getDeviceId().getFullPath());
      if (endTime == null || endTime < maxTime) {
        tempEndTimeMap.put(plan.getDeviceId().getFullPath(), maxTime);
      }
    }
    MeasurementMNode[] mNodes;
    try {
      mNodes = IoTDB.metaManager.getMNodes(plan.getDeviceId(), plan.getMeasurements());
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    //set measurementMNodes, WAL already serializes the real data type, so no need to infer type
    plan.setMeasurementMNodes(mNodes);
    //mark failed plan manually
    checkDataTypeAndMarkFailed(mNodes, plan);
    if (plan instanceof InsertRowPlan) {
      recoverMemTable.insert((InsertRowPlan) plan);
    } else {
      recoverMemTable.insertTablet((InsertTabletPlan) plan, 0, ((InsertTabletPlan) plan).getRowCount());
    }
  }

  @SuppressWarnings("unused")
  private void replayUpdate(UpdatePlan updatePlan) {
    // TODO: support update
    throw new UnsupportedOperationException("Update not supported");
  }

  private void checkDataTypeAndMarkFailed(final MeasurementMNode[] mNodes, InsertPlan tPlan) {
    for (int i = 0; i < mNodes.length; i++) {
      if (mNodes[i] == null) {
        tPlan.markFailedMeasurementInsertion(i,
            new PathNotExistException(tPlan.getDeviceId().getFullPath() +
                IoTDBConstant.PATH_SEPARATOR + tPlan.getMeasurements()[i]));
      } else if (mNodes[i].getSchema().getType() != tPlan.getDataTypes()[i]) {
        tPlan.markFailedMeasurementInsertion(i,
            new DataTypeMismatchException(mNodes[i].getName(), tPlan.getDataTypes()[i],
                mNodes[i].getSchema().getType()));
      }
    }
  }

  public class WalHelper{
    private TSDataType type;
    private Object value;

    public WalHelper(TSDataType type, Object value) {
      this.type = type;
      this.value = value;
    }

    public TSDataType getType() {
      return type;
    }

    public Object getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "WalHelper{" +
              "type='" + type + '\'' +
              ", value=" + value +
              '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WalHelper walHelper = (WalHelper) o;
      return Objects.equals(type, walHelper.type) && Objects.equals(value, walHelper.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, value);
    }
  }

  public void writeCsvs(Map<String,Map<Long,Map<String,WalHelper>>> allData, String folder){
    for(Map.Entry<String,Map<Long,Map<String,WalHelper>>> mainEntry: allData.entrySet()){
      String file = String.format("%s/%s.txt",folder,mainEntry.getKey());
      Set<Long> timestampSet = new LinkedHashSet<>();
      for(Map.Entry<Long,Map<String,WalHelper>> subEntry: mainEntry.getValue().entrySet()){

        timestampSet.add(subEntry.getKey());

//        for(Map.Entry<String,WalHelper> subsubEntry: subEntry.getValue().entrySet()){
//          columnSet.add(subsubEntry.getKey());
//
//        }
      }
      List<Long> tsList = new ArrayList<>(timestampSet);
      List<Long> sortedTs = tsList.stream().sorted().collect(Collectors.toList());



      List<String> queries = new ArrayList<>();
      int tsCounter = 0;
      int itemCounter = 0;
      for(Long ts: sortedTs){
        tsCounter++;
        List<String> cols = new ArrayList<>();
        List<WalHelper> walHelperList = new ArrayList<>();
        System.out.println("ts: " + tsCounter + " / " + sortedTs.size() + ", items: " + itemCounter);
        for(Map.Entry<String,WalHelper> subEntry: mainEntry.getValue().get(ts).entrySet()){
          cols.add(subEntry.getKey());
          walHelperList.add(subEntry.getValue());
        }
        String query = String.format("INSERT into %s.default(time",mainEntry.getKey());
        for(String col:cols){
          itemCounter++;
//          if(first) {
//            query = String.format("%s`%s`",query,col);
//            first=false;
//          }
//          else{
            query = String.format("%s,`%s`",query,col);
//          }
        }
        query = String.format("%s) VALUES(%s",query,ts);
        String val;
        for(WalHelper helper:walHelperList){


          switch (helper.getType()) {
            case DOUBLE:
              val = String.format("%6.3f", (double) helper.getValue());
              break;
            case FLOAT:
              val = String.format("%6.3f", (float) helper.getValue());
              break;
            case BOOLEAN:
              val = "true";
              if (!(boolean) helper.getValue()) {
                val = "false";
              }
              break;
            case TEXT:
              val = String.format("'%s'", helper.getValue());
              break;
            case INT32:
              val = String.format("%d", (int) helper.getValue());
              break;
            case INT64:
              val = String.format("%d", (long) helper.getValue());
              break;
            default:
              logger.warn("Could not find the given type assume as empty");
              val = "";
          }

//          if(first) {
//            query = String.format("%s%s",query,val);
//            first=false;
//          }
//          else{
          query = String.format("%s,%s",query,val);


        }
        query = String.format("%s)",query);
//        System.out.println("query: " + query);
        queries.add(query);
      }
      BufferedWriter writer = null;
      try {
        writer = new BufferedWriter(new FileWriter(file));
        for(String line:queries){
          writer.write(String.format("%s\n",line));
        }

        writer.close();
      } catch (IOException e) {
        logger.warn("Could not write to file",e);
      }






    }
  }
}
