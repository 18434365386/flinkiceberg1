package com.dx.datastream;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * Flink DataStreamAPI操作批量和实时读取Iceberg表数据
 */
public class FlinkIcebergRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.配置TableLoader
        Configuration hadoopConf = new Configuration();
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://leidi01:8020/flinkiceberg//icebergdb/flink_iceberg_tbl", hadoopConf);

        //2.从Iceberg中读取全量/增量读取数据
        DataStream<RowData> batchData = FlinkSource.forRowData().env(env)
                .tableLoader(tableLoader)
                //基于某个快照实时增量读取数据，快照需要从元数据中获取
                .startSnapshotId(1738199999360637062L)
                //默认为false,整批次读取; 设置为true为流式读取
                .streaming(true)
                .build();

        batchData.map(new MapFunction<RowData, String>() {
            @Override
            public String map(RowData rowData) throws Exception {
                int id = rowData.getInt(0);
                String name = rowData.getString(1).toString();
                int age = rowData.getInt(2);
                String loc = rowData.getString(3).toString();
                return id+","+name+","+age+","+loc;
            }
        }).print();

        env.execute("DataStream Api Read Data From Iceberg");

    }
}