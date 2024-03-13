package at.ac.uibk.dps.streamprocessingapplications.tasks;

import at.ac.uibk.dps.streamprocessingapplications.entity.azure.Taxi_Trip;
import at.ac.uibk.dps.streamprocessingapplications.utils.TaxiDataGenerator;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableQuery;
import java.util.*;
import org.slf4j.Logger;

public class AzureTableRangeQueryTaskTAXI extends AbstractTask {

    private static final Object SETUP_LOCK = new Object();
    // TODO: remove init values after config.properties has been initialized
    private static String storageConnStr;
    private static String tableName;
    private static String partitionKey;
    private static boolean doneSetup = false;
    private static int startRowKey;
    private static int endRowKey;

    private static int useMsgField;
    private static Random rn;

    public void setup(Logger l_, Properties p_) {
        super.setup(l_, p_);
        synchronized (SETUP_LOCK) {
            if (!doneSetup) { // Do setup only once for this task
                storageConnStr =
                        p_.getProperty(
                                "IO.AZURE_STORAGE_CONN_STR"); // TODO: add to config.property file
                tableName =
                        p_.getProperty(
                                "IO.AZURE_TABLE.TABLE_NAME"); // TODO: pass table with TaxiDropoff
                // Entity
                partitionKey =
                        p_.getProperty("IO.AZURE_TABLE.PARTITION_KEY"); // TODO: pass partition with
                // TaxiDropoff Entity
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
        if (l.isInfoEnabled()) l.info("Table name is - " + cloudTbl.getName());
        // FIXME: How do you advance the rowkey. Have a start and end for row key as input property?
        //		String rowKeyStart,rowKeyEnd;
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
        Iterable<Taxi_Trip> result =
                getAzTableRangeByKeyTAXI(cloudTbl, partitionKey, rowKeyStart, rowKeyEnd, l);
        //		System.out.println("Row key = "+ rowKeyEnd);
        //		System.out.println("Result = "+ result);

        super.setLastResult(result);

        return Float.valueOf(Lists.newArrayList(result).size()); // may need updation
    }

    public Float doTaskLogicDummy(Map map) {
        String rowKeyStart = (String) map.get("ROWKEYSTART");
        String rowKeyEnd = (String) map.get("ROWKEYEND");
        long start = Long.parseLong(rowKeyStart);
        long end = Long.parseLong(rowKeyEnd);

        List<Taxi_Trip> resultList = new ArrayList<>();
        /*
        for (long i = start; start <= end; i++) {
            resultList.add(FitDataGenerator.generateRandomFITData());

        }
         */
        for (long i = 0; i <= 10; i++) {
            // FIXME!
            resultList.add(TaxiDataGenerator.getNextDataEntry());
        }

        Iterable<Taxi_Trip> result = resultList;

        super.setLastResult(result);

        return Float.valueOf(Lists.newArrayList(result).size()); // may need updation
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
    public static Iterable<Taxi_Trip> getAzTableRangeByKeyTAXI(
            CloudTable cloudTable,
            String partitionKey,
            String rowkeyStart,
            String rowkeyEnd,
            Logger l) {

        try {

            // filters
            System.out.println("getAzTableRowByKey-" + rowkeyStart + "," + rowkeyEnd);

            // Create a filter condition where the partition key is "Smith".
            String partitionFilter =
                    TableQuery.generateFilterCondition(
                            "PartitionKey", TableQuery.QueryComparisons.EQUAL, "taxi_range");

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

            String filterRange =
                    TableQuery.combineFilters(filter2, TableQuery.Operators.AND, filter3);

            // Combine the two conditions into a filter expression.
            String combinedFilter =
                    TableQuery.combineFilters(
                            partitionFilter, TableQuery.Operators.AND, filterRange);

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

            TableQuery<Taxi_Trip> rangeQuery =
                    TableQuery.from(Taxi_Trip.class).where(combinedFilter);

            Iterable<Taxi_Trip> queryRes = cloudTable.execute(rangeQuery);

            //			// Loop through the results, displaying information about the entity
            //			for (Taxi_Trip entity : queryRes) {
            //				System.out.println(entity.getPartitionKey() +
            //						" " + entity.getRangeKey() +
            //						"\t" + entity.getAirquality_raw()
            //				);
            //			}

            return queryRes;
        } catch (Exception e) {
            l.warn(
                    "Exception in getAzTableRowByKey:" + cloudTable + "; rowkeyEnd: " + rowkeyEnd,
                    e);
        }
        return null;
    }

    /***
     *
     */

    //	public static  final class TaxiDropoffEntity extends TableServiceEntity { // FIXME: This is
    // specific to the input data in table
    //
    //		public TaxiDropoffEntity(){}
    //
    //		String Dropoff_datetime;
    //	    String Dropoff_longitude;
    //		String Fare_amount;
    //		String Airquality_raw;
    //
    //
    //	    public String getDropoff_datetime() {
    //	        return this.Dropoff_datetime;
    //	    }
    //	    public String getDropoff_longitude() {
    //	        return this.Dropoff_longitude;
    //	    }
    //		public String getFare_amount() {
    //			return this.Fare_amount;
    //		}
    //		public String getAirquality_raw() {
    //			return this.Airquality_raw;
    //		}
    //
    //	    public void setDropoff_datetime(String Dropoff_datetime) {
    //	        this.Dropoff_datetime = Dropoff_datetime;
    //	    }
    //	    public void setDropoff_longitude(String Dropoff_longitude) {
    //	        this.Dropoff_longitude = Dropoff_longitude;
    //	    }
    //		public void setFare_amount(String Fare_amount) {
    //			this.Fare_amount = Fare_amount;
    //		}
    //		public void setAirquality_raw(String Airquality_raw) {
    //			this.Airquality_raw = Airquality_raw;
    //		}
    //
    //		String Temperature;
    //		public String getTemperature() {
    //			return this.Temperature;
    //		}
    //		public void setTemperature(String Temperature) {
    //			this.Temperature = Temperature;
    //		}
    //	}

}
