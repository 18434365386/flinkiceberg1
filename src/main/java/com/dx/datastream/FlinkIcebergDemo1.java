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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import java.util.Map;

/**
 * 使用DataStream Api 向Iceberg 表写入数据
 */
public class FlinkIcebergDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.必须设置checkpoint ,Flink向Iceberg中写入数据时当checkpoint发生后，才会commit数据。
        env.enableCheckpointing(5000);

        //2.读取Kafka 中的topic 数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.6.102:6667")
                .setTopics("json")
                .setGroupId("my-group-id")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //3.对数据进行处理，包装成RowData 对象，方便保存到Iceberg表中。
        SingleOutputStreamOperator<RowData> dataStream = kafkaSource.map(new MapFunction<String, RowData>() {
            @Override
            public RowData map(String s) throws Exception {
                System.out.println("s = "+s);
                String[] split = s.split(",");
                GenericRowData row = new GenericRowData(4);
                row.setField(0, Integer.valueOf(split[0]));
                row.setField(1, StringData.fromString(split[1]));
                row.setField(2, Integer.valueOf(split[2]));
                row.setField(3, StringData.fromString(split[3]));
                return row;
            }
        });

        //4.创建Hadoop配置、Catalog配置和表的Schema，方便后续向路径写数据时可以找到对应的表
        Configuration hadoopConf = new Configuration();
        Catalog catalog = new HadoopCatalog(hadoopConf,"hdfs://leidi01:8020/flinkiceberg/");

        //配置iceberg 库名和表名
        TableIdentifier name =
                TableIdentifier.of("icebergdb", "flink_iceberg_tbl");

        //创建Icebeng表Schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "nane", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get()),
                Types.NestedField.required(4, "loc", Types.StringType.get()));

        //如果有分区指定对应分区，这里“loc”列为分区列，可以指定unpartitioned 方法不设置表分区
//        PartitionSpec spec = PartitionSpec.unpartitioned();
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("loc").build();

        //指定Iceberg表数据格式化为Parquet存储
        Map<String, String> props =
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());
        Table table = null;

        // 通过catalog判断表是否存在，不存在就创建，存在就加载
        if (!catalog.tableExists(name)) {
            table = catalog.createTable(name, schema, spec, props);
        }else {
            table = catalog.loadTable(name);
        }

        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://leidi01:8020/flinkiceberg//icebergdb/flink_iceberg_tbl", hadoopConf);

        //5.通过DataStream Api 向Iceberg中写入数据
        FlinkSink.forRowData(dataStream)
                //这个 .table 也可以不写，指定tableLoader 对应的路径就可以。
                .table(table)
                .tableLoader(tableLoader)
                //默认为false,追加数据。如果设置为true 就是覆盖数据
                .overwrite(false)
                .build();

        env.execute("DataStream Api Write Data To Iceberg");
    }
}