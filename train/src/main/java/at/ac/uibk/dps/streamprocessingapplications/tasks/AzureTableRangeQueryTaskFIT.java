package at.ac.uibk.dps.streamprocessingapplications.tasks;

import at.ac.uibk.dps.streamprocessingapplications.entity.azure.FIT_data;
import at.ac.uibk.dps.streamprocessingapplications.utils.FitDataGenerator;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableQuery;
import java.util.*;
import org.slf4j.Logger;

public class AzureTableRangeQueryTaskFIT extends AbstractTask {
  private static final Object SETUP_LOCK = new Object();
  private static String storageConnStr;
  private static String tableName;
  private static String partitionKey;
  private static boolean doneSetup = false;
  private static int startRowKey;
  private static int endRowKey;

  private static int useMsgField;
  private static Random rn;
  private final String dataSetPath;
  private boolean isJson;

  public AzureTableRangeQueryTaskFIT(String dataSetPath) {
    this.dataSetPath = dataSetPath;
    this.setJson(dataSetPath.contains("senml"));
  }

  /***
   *
   * @param azStorageConnStr
   * @param tableName
   * @param l
   * @return
   */
  public static CloudTable connectToAzTable(String azStorageConnStr, String tableName, Logger l) {
    CloudTable cloudTable = null;
    try {
      // Retrieve storage account from connection-string.
      CloudStorageAccount storageAccount = CloudStorageAccount.parse(azStorageConnStr);

      // Create the table client
      CloudTableClient tableClient = storageAccount.createCloudTableClient();

      // Create a cloud table object for the table.
      cloudTable = tableClient.getTableReference(tableName);
    } catch (Exception e) {
      l.warn("Exception in connectToAzTable: " + tableName, e);
    }
    return cloudTable;
  }

  /***
   *
   * @param cloudTable
   * @param partitionKey
   * //	 * @param rowkey
   * @param l
   * @return
   */
  public static Iterable<FIT_data> getAzTableRangeByKeyFIT(
      CloudTable cloudTable, String partitionKey, String rowkeyStart, String rowkeyEnd, Logger l) {

    try {

      // filters
      // System.out.println("getAzTableRowByKey-" + rowkeyStart + "," + rowkeyEnd);

      // Create a filter condition where the partition key is "Smith".
      String partitionFilter =
          TableQuery.generateFilterCondition(
              "PartitionKey", TableQuery.QueryComparisons.EQUAL, "fit_range");

      String filter2 =
          TableQuery.generateFilterCondition(
              "RangeTs",
              TableQuery.QueryComparisons.GREATER_THAN_OR_EQUAL,
              Long.parseLong(rowkeyStart)); // recordStart i.e.: "123"
      String filter3 =
          TableQuery.generateFilterCondition(
              "RangeTs",
              TableQuery.QueryComparisons.LESS_THAN,
              Long.parseLong(rowkeyEnd)); // recordEnd i.e.: "567"

      String filterRange = TableQuery.combineFilters(filter2, TableQuery.Operators.AND, filter3);

      // Combine the two conditions into a filter expression.
      String combinedFilter =
          TableQuery.combineFilters(partitionFilter, TableQuery.Operators.AND, filterRange);

      //			TableQuery<TaxiDropoffEntity> rangeQuery =
      //					TableQuery.from(TaxiDropoffEntity.class)
      //							.where(combinedFilter);

      //					Iterable<TaxiDropoffEntity> queryRes = cloudTable.execute(rangeQuery);
      //			// Loop through the results, displaying information about the entity
      //			for (TaxiDropoffEntity entity : queryRes) {
      //				System.out.println(entity.getPartitionKey() +
      //						" " + entity.getRowKey() +
      //						"\t" + entity.Temperature
      //						);
      //			}

      TableQuery<FIT_data> rangeQuery = TableQuery.from(FIT_data.class).where(combinedFilter);

      Iterable<FIT_data> queryRes = cloudTable.execute(rangeQuery);

      //			// Loop through the results, displaying information about the entity
      //			for (FIT_data entity : queryRes) {
      //				System.out.println(entity.getPartitionKey() +
      //						" " + entity.getRangeKey() +
      //						"\t" + entity.getAirquality_raw()
      //				);
      //			}

      return queryRes;
    } catch (Exception e) {
      l.warn("Exception in getAzTableRowByKey:" + cloudTable + "; rowkeyEnd: " + rowkeyEnd, e);
    }
    return null;
  }

  public void setJson(boolean json) {
    isJson = json;
  }

  public void setup(Logger l_, Properties p_) {
    super.setup(l_, p_);
    synchronized (SETUP_LOCK) {
      if (!doneSetup) { // Do setup only once for this task
        storageConnStr = p_.getProperty("IO.AZURE_STORAGE_CONN_STR");
        tableName = p_.getProperty("IO.AZURE_TABLE.TABLE_NAME");
        // Entity
        partitionKey = p_.getProperty("IO.AZURE_TABLE.PARTITION_KEY");
        useMsgField =
            Integer.parseInt(
                p_.getProperty(
                    "IO.AZURE_TABLE.USE_MSG_FIELD",
                    "0")); // If positive, use that particular field number in
        // the input CSV message as input for count
        startRowKey = Integer.parseInt(p_.getProperty("IO.AZURE_TABLE.START_ROW_KEY"));
        endRowKey = Integer.parseInt(p_.getProperty("IO.AZURE_TABLE.END_ROW_KEY"));
        rn = new Random();
        doneSetup = true;
      }
    }
  }

  @Override
  protected Float doTaskLogic(Map map) {
    String rowKeyStart, rowKeyEnd;
    CloudTable cloudTbl = connectToAzTable(storageConnStr, tableName, l);
    l.warn("Table name is - " + cloudTbl.getName());
    if (useMsgField > 0) {
      //			rowKey = m.split(",")[useMsgField - 1];
      rowKeyStart = (String) map.get("ROWKEYSTART");
      rowKeyEnd = (String) map.get("ROWKEYEND");
      //			assert Integer.parseInt(rowKeyStart)>=startRowKey;
      //			assert Integer.parseInt(rowKeyEnd)<=endRowKey;
      if (l.isInfoEnabled()) l.info("1-row key accesed till - " + rowKeyEnd);
    } else {
      rowKeyStart = String.valueOf(rn.nextInt(endRowKey));
      rowKeyEnd = String.valueOf(rn.nextInt(endRowKey));
      if (l.isInfoEnabled()) l.info("2-row key accesed - " + rowKeyEnd);
    }
    Iterable<FIT_data> result =
        getAzTableRangeByKeyFIT(cloudTbl, partitionKey, rowKeyStart, rowKeyEnd, l);

    super.setLastResult(result);

    return (float) Lists.newArrayList(result).size(); // may need updation
  }

  public Float doTaskLogicDummy(Map map) {
    String rowKeyStart = (String) map.get("ROWKEYSTART");
    String rowKeyEnd = (String) map.get("ROWKEYEND");
    long start = Long.parseLong(rowKeyStart);
    long end = Long.parseLong(rowKeyEnd);

    List<FIT_data> resultList = new ArrayList<>();
    /*
    for (long i = start; start <= end; i++) {
        resultList.add(FitDataGenerator.generateRandomFITData());

    }
     */
    FitDataGenerator fitDataGenerator = new FitDataGenerator(dataSetPath, isJson);
    for (long i = 0; i <= 10; i++) {
      resultList.add(fitDataGenerator.getNextDataEntry());
    }

    super.setLastResult(resultList);

    return (float) Lists.newArrayList(resultList).size();
  }
}
