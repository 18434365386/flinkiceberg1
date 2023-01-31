package com.dx.datastream;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

public class FlinkIcebergDemo2 {
    public static void main(String[] args) {
        // 1、设置Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2、设置Checkpoint, 注意Iceberg的Commit时间必须在CheckPoint完成后才能执行
        env.enableCheckpointing(5000);

        // 3、读取Kafka中数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.6.102:6667")
                .setTopics("json")
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        // 4、加载Kafka数据源,设置Watermark为空
        DataStreamSource<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 5、将Kafka中数据写入Iceberg中,将数据封装为RowData / Data
        SingleOutputStreamOperator<RowData> kafkaDS = dataStream.map(new MapFunction<String, RowData>() {
            @Override
            public RowData map(String line) throws Exception {
                String[] split = line.split("-");

                // 5.1、新建一个RowData对象
                GenericRowData row = new GenericRowData(2);

                // 5.2、格式转换后数据插入RowData
                row.setField(0, StringData.fromString(split[0]));
                row.setField(1, StringData.fromString(split[1]));

                return row;
            }
        });

        // 6、创建Hadoop配置、Catalog配置和表的Schema,之后创建Iceberg表
        Configuration hadoopConf = new Configuration();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, "hdfs://leidi01:8020/iceberg");
        // 6.1、配置iceberg库名和表名
        TableIdentifier name = TableIdentifier.of("icebergdb", "flink_iceberg_demo1");
        // 6.2、创建Iceberg表Schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "topic", Types.StringType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get())
        );
        /**
         * 6.3、指定分区
         * ①第一种：unpartitioned方法表示不指定分区
         * ②第二种：设置topic列为分区列
         */
//        PartitionSpec unpartitioned = PartitionSpec.unpartitioned();
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("topic").build();
        // 6.4、指定Iceberg表数据格式为Parquet存储
        ImmutableMap<String, String> prop = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());

        // 6.5、通过Catalog判断表是否存在,不存在则创建,存在则加载
        Table table = null;
        if(!hadoopCatalog.tableExists(name)){
            table = hadoopCatalog.createTable(name,schema,spec,prop);
        } else {
            table = hadoopCatalog.loadTable(name);
        }

        // 7、创建Iceberg表对象
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://leidi01:8020/iceberg/icebergdb/flink_iceberg_demo1");

        // 8、将流式结果写出到Iceberg表中
        FlinkSink.forRowData(kafkaDS)
                .table(table)
                .tableLoader(tableLoader)
                .overwrite(false)
                .build();
    }
}