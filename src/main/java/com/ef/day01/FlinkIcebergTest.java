package com.ef.day01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;

/**
 * @author:erfeng_v
 * @create: 2023-01-31 22:35
 * @Description:
 */
public class FlinkIcebergTest {

    public static void main(String[] args) {
        //1.创建flink环境
        StreamExecutionEnvironment environment =StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置checkpoint
        environment.enableCheckpointing(5000);

        //3.flink读取kafka中数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                //指定集群节点
                .setBootstrapServers("node1:9092,node2:9092")
                .setTopics("flink-iceberg-topic")
                .setGroupId("my-group-id")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        //从当前kafka中读取到的数据
        DataStreamSource<String> kafka_source = environment.fromSource(source,
                WatermarkStrategy.noWatermarks(), "kafka source");

        SingleOutputStreamOperator<RowData> dataStream = kafka_source.map(new MapFunction<String, RowData>() {
            @Override
            public RowData map(String line) throws Exception {
                String[] split = line.split(",");
                GenericRowData row = new GenericRowData(4);
                row.setField(0, Integer.valueOf(split[0]));
                row.setField(1, StringData.fromString(split[1]));
                row.setField(2, Integer.valueOf(split[2]));
                row.setField(3, StringData.fromString(split[3]));
                return row;
            }
        });

        //4.Flink 创建iceberg表
        //创建Hadoop配置，Catalog配置和表的scheme，方便后续向路径写数据时可以找到对应的表
        Configuration hadoopConf = new Configuration();

//        Catalog catalog =new HadoopCatalog


    }
}
