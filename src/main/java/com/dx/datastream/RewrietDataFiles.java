package com.dx.datastream;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.hadoop.HadoopCatalog;


/**
 * 通过Flink批量任务来合并Data File文件
 */
public class RewrietDataFiles {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 1、配置TableLoader
        Configuration hadoopConf = new Configuration();
        //2.创建Hadoop配置、Catalog配置和表的Schema，方便后续向路径写数据时可以找到对应的表
        Catalog catalog = new HadoopCatalog(hadoopConf,"hdfs://leidi01:8020/flinkiceberg/");

        //3.配置iceberg 库名和表名并加载表
        TableIdentifier name = TableIdentifier.of("icebergdb", "flink_iceberg_tbl");
        Table table = catalog.loadTable(name);

        //4..合并 data files 小文件
        RewriteDataFilesActionResult result = Actions.forTable(table)
                .rewriteDataFiles()
                //默认 512M ，可以手动通过以下指定合并文件大小，与Spark中一样。
                .targetSizeInBytes(536870912L)
                .execute();

    }
}
