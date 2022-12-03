//snippet-sourcedescription:[StartQueryExample.java demonstrates how to submit a query to Amazon Athena for execution, wait until the results are available, and then process the results.]
//snippet-keyword:[AWS SDK for Java v2]
//snippet-keyword:[Code Sample]
//snippet-keyword:[Amazon Athena]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[11/02/2020]
//snippet-sourceauthor:[scmacdon - aws]
/*
   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   SPDX-License-Identifier: Apache-2.0
*/

//snippet-start:[athena.java2.StartQueryExample.complete]
//snippet-start:[athena.java.StartQueryExample.complete]
package aws.example.inventory;

//snippet-start:[athena.java2.StartQueryExample.import]
import com.fasterxml.jackson.databind.util.JSONPObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;
import software.amazon.awssdk.services.pricing.PricingClient;
import software.amazon.awssdk.services.pricing.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
//snippet-end:[athena.java2.StartQueryExample.import]

public class StartQueryExample {
    //snippet-start:[athena.java2.StartQueryExample.main]

    public static final String ATHENA_DEFAULT_DATABASE = "DEFAULT";
    private static final String CREATE_TABLE_SQL_TEMPLATE = "CREATE EXTERNAL TABLE %s(" +
            "         bucket string," +
            "         key string," +
            "         is_latest boolean," +
            "         size string," +
            "         last_modified_date string," +
            "         storage_class string" +
            ") PARTITIONED BY (" +
            "        dt string" +
            ")" +
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'" +
            "  STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'" +
            "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" +
            "  LOCATION 's3://%s/%s/%s/%s/hive/'" +
            "  TBLPROPERTIES (" +
            "    \"projection.enabled\" = \"true\"," +
            "    \"projection.dt.type\" = \"date\"," +
            "    \"projection.dt.format\" = \"yyyy-MM-dd-HH-mm\"," +
            "    \"projection.dt.range\" = \"%s,NOW\"," +
            "    \"projection.dt.interval\" = \"1\"," +
            "    \"projection.dt.interval.unit\" = \"DAYS\"" +
            "  );";

    private static final String COST_ANALYTICS_SQL_TEMPLATE = "select GB,storage_class, GB*price as cost, price from\n" +
            "(select GB,storage_class, case %s else 0 end as price\n" +
            "    from\n" +
            "(select sum(size)/1073741824 as GB, storage_class from %s group by storage_class))";
    public static void main(String[] args) {
        // create inventory table
//        createTable();
//        execute("my_destination_bucket_name", "SELECT * FROM my_table_name limit 100");
        execute("my_destination_bucket_name", generateSQLbyPrice("new1", COST_ANALYTICS_SQL_TEMPLATE, getS3Pricing()));
    }

    private static String generateSQLbyPrice(String tableName, String costAnalyticsSqlTemplate, Map<String, Double> s3Pricing) {
        StringBuffer buffer = new StringBuffer();
        s3Pricing.keySet().forEach(key -> {
            buffer.append(String.format("when storage_class='%s' then %f ", key, s3Pricing.get(key)));
        });
        return String.format(costAnalyticsSqlTemplate, buffer.toString(), tableName);
    }

    private static Map<String, Double> getS3Pricing(){
        Map<String, Double> priceList = new HashMap<>();
        PricingClient pricingClient = PricingClient.create();
        pricingClient.getProducts(GetProductsRequest.builder()
                        .serviceCode("AmazonS3")
                        .filters(Filter.builder()
                                .type(FilterType.TERM_MATCH)
                                .field("productFamily").value("Storage")
                                .build(),
                                Filter.builder()
                                .type(FilterType.TERM_MATCH)
                                .field("regionCode").value("cn-north-1")
                                .build())
                .build()).priceList().stream()
                .forEach(price -> insertPriceData(priceList, price));
        return priceList;
    }

    private static void insertPriceData(Map<String, Double> priceList, String price) {
        String key = ConvertStorageTag(price.split("\"volumeType\":\"")[1].split("\"")[0]);
        String value = price.split("\"CNY\":\"")[1].split("\"")[0];
        priceList.put(key, Double.valueOf(value));
    }

    private static String ConvertStorageTag(String s) {
        return s.replaceAll(" ", "_").toUpperCase();
    }

    private static void createTable(){
        String sourceBucket = "my_source_bucket_name";
        String destBucket = "my_destination_bucket_name";
        String prefix = "your_prefix";
        String startDate = "2022-11-28-00-00";
        execute(destBucket, String.format(CREATE_TABLE_SQL_TEMPLATE, "new", destBucket, sourceBucket, sourceBucket, prefix, startDate));
    }

    public static void execute(String destBucket, String sql){
        System.out.println(sql);
        AthenaClient athenaClient = AthenaClient.builder()
                .region(Region.US_WEST_2)
                .build();

        String queryExecutionId = submitAthenaQuery(athenaClient, sql, destBucket);
        try {
            waitForQueryToComplete(athenaClient, queryExecutionId);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        processResultRows(athenaClient, queryExecutionId);
        athenaClient.close();
    }

    // Submits a sample query to Amazon Athena and returns the execution ID of the query
    public static String submitAthenaQuery(AthenaClient athenaClient, String sql, String destBucket) {

        try {

            // The QueryExecutionContext allows us to set the database
            QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                .database(ATHENA_DEFAULT_DATABASE).build();

            // The result configuration specifies where the results of the query should go
            ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                    .outputLocation("s3://"+destBucket)
                    .build();

            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                    .queryString(sql)
                    .queryExecutionContext(queryExecutionContext)
                .   resultConfiguration(resultConfiguration)
                    .build();

            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            return startQueryExecutionResponse.queryExecutionId();

        } catch (AthenaException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return "";
    }

    // Wait for an Amazon Athena query to complete, fail or to be cancelled
    public static void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("The Amazon Athena query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again
                Thread.sleep(1000);
            }
            System.out.println("The current status is: " + queryState);
        }
    }

    // This code retrieves the results of a query
    public static void processResultRows(AthenaClient athenaClient, String queryExecutionId) {

       try {

           // Max Results can be set but if its not set,
           // it will choose the maximum page size
            GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                    .queryExecutionId(queryExecutionId)
                    .build();

            GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

            for (GetQueryResultsResponse result : getQueryResultsResults) {
                List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
                List<Row> results = result.resultSet().rows();
                processRow(results, columnInfoList);
            }

        } catch (AthenaException e) {
           e.printStackTrace();
           System.exit(1);
       }
    }

    private static void processRow(List<Row> row, List<ColumnInfo> columnInfoList) {

        for (Row myRow : row) {
            List<Datum> allData = myRow.data();
            for (Datum data : allData) {
                System.out.print(data.varCharValue()+",");
            }
            System.out.println();
        }
    }
    //snippet-end:[athena.java2.StartQueryExample.main]
}
//snippet-end:[athena.java.StartQueryExample.complete]

//snippet-end:[athena.java2.StartQueryExample.complete]