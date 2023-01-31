package com.ef.day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author:erfeng_v
 * @create: 2023-01-31 23:11
 * @Description: Flink SQL API实时读取Kafka 写入到Iceberg
 */
public class FlinkIcebergSql {

    public static void main(String[] args) {
        //1.创建flink环境
        StreamExecutionEnvironment environment =StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(environment);

        Configuration configuration = tblEnv.getConfig().getConfiguration();
        //支持SQL语法中的OPTIONS选项
        configuration.setBoolean("table.dynamic-table-options.enabled",true);

        environment.enableCheckpointing(10000);

        //创建Catalog
        tblEnv.executeSql(
                "create catalog hadoop_iceberg with " +
                        "('type'='iceberg'," +
                        "'catalog-type'='hadoop'," +
                        "'warehouse'='hdfs://mycluster/flink_iceberg')"
        );
        //创建iceberg表
        tblEnv.executeSql("create table hadoop_iceberg.iceberg_db.flink_iceberg_table (" +
                "id int," +
                "name string," +
                "age int," +
                "loc string) partitioned by (loc)");


        //3.读取kafka中的数据
        tblEnv.executeSql("create table kafka_input_table(" +
                "id int," +
                "name varchar," +
                "age int," +
                "loc varchar" +
                ") with (" +
                "'connector'='kafka'," +
                "'topic'='flink_iceberg-topic'," +
                "'properties.bootstrap.servers'='node1:9092,node2:9092'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='csv'," +
                "'properties.group.id'='my-group-id'" +
                ")");

        //4.向Iceberg表中写数据
        tblEnv.executeSql("insert into hadoop_iceberg.iceberg_db.flink_iceberg_table" +
                "select id ,name,age,loc from kafka_input_table ");

        //5.查询Iceberg表数据并打印


         }


}
