package org.ksm.integration;

import io.delta.tables.DeltaTable;
import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Assert;
import org.junit.Test;

public class SparkUtilsTest extends TestCase {

    @Test
    public void testDelta() {
        try {

            SparkSession sparkSession = SparkUtils.getSparkSession();
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

            long count = sparkSession.sql("select * from employee").count();

            Assert.assertTrue(count >= 50);

        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
    }
}