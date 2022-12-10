package org.ksm.integration;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;


public class SparkUtils {

    /**
     * create spark session with delta extensions and custom DeltaCatalog Class
     * ***/
    public static SparkSession getSparkSession() throws NoSuchTableException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("CDPDelta")
                .master("local[1]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.CDPDeltaCatalog")
                .enableHiveSupport()
                .getOrCreate();

        return sparkSession;
    }

    /**
     * create external table with delta and cloudera spark with custom DeltaCatalog Class
     * ***/
    public static void main(String[] args) throws NoSuchTableException {

        SparkSession sparkSession = getSparkSession();
        sparkSession.sql("show tables").show(false);

        Dataset<Row> csv = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/main/resources/employee.csv");

        sparkSession.sql("CREATE OR REPLACE TABLE employee\n" +
                "(EMPLOYEE_ID int,\n" +
                "FIRST_NAME string,\n" +
                "LAST_NAME string,\n" +
                "EMAIL string,\n" +
                "PHONE_NUMBER string,\n" +
                "HIRE_DATE string,\n" +
                "JOB_ID string,\n" +
                "SALARY int,\n" +
                "COMMISSION_PCT string,\n" +
                "MANAGER_ID string,\n" +
                "DEPARTMENT_ID int) USING DELTA location '/tmp/test/deltatable'").show(false);

        csv.writeTo("employee").append();
        sparkSession.sql("show tables").show(false);

        DeltaTable deltaTable = DeltaTable.forName("employee");
        deltaTable.history().show(false);

        System.out.println("");

    }

}
