package com.ruozedata;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;
import org.apache.kafka.common.TopicPartition;
import java.util.Map;
import java.lang.*;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.*;


public class SparkStreamingKafkaOffsetToSaveHBase {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS");
        System.out.println("启动时间："+format.format( new Date()));

        JavaInputDStream<ConsumerRecord<String, String>> stream = null;
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.toLevel("WARN"));

        String brokers   = "192.168.17.1:9092,192.168.17.2:9092,192.168.17.3:9092";  //Kafka brokers list， 用逗号分割
        String topics    = "ruozedata";  //要消费的话题，目前仅支持一个，想要扩展很简单，不过需要设计一下保存offset的表，请自行研究
        String groupid   = "ruozedatagroup";  //指定消费者group
        String zklist    = "192.168.17.1:2181,192.168.17.2:2181,192.168.17.3:2181";  //hbase连接要用到zookeeper server list，用逗号分割
        String datatable = "kafkaoffset";  //想要保存消息offset的hbase数据表

        final   SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaOffsetSaveToHBase").setMaster("local[2]");
        final  JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupid);
        kafkaParams.put("auto.offset.reset", "latest"); //默认第一次执行应用程序时从Topic的首位置开始消费
        kafkaParams.put("enable.auto.commit", "false");   //不使用kafka自动提交模式，由应用程序自己接管offset

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));  //此处把1个或多个topic解析为集合对象，因为后面接口需要传入Collection类型

        //建立hbase连接
        Configuration conf = HBaseConfiguration.create(); // 获得配置文件对象
        conf.set("hbase.zookeeper.quorum", zklist);
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        //获得连接对象
        Connection con = ConnectionFactory.createConnection(conf);
        Admin admin = con.getAdmin();

        //System.out.println(  " @@@@@@@@ " + admin ); //调试命令，判断连接是否成功
        TableName tn = TableName.valueOf(datatable); //创建表名对象


        /*存放offset的表模型如下，请自行优化和扩展，请把每个rowkey对应的record的version设置为1（默认值），因为要覆盖原来保存的offset，而不是产生多个版本
         *----------------------------------------------------------------------------------------------
         *  rowkey           |  column family                                                          |
         *                   --------------------------------------------------------------------------
         *                   |  column:topic(string)  |  column:partition(int)  |   column:offset(long)|
         *----------------------------------------------------------------------------------------------
         * 业务线_topic_partition   |   topic                |   partition             |    offset            |
         *----------------------------------------------------------------------------------------------
         * ruoze_p0              ruoze                   po                           101
		 */

        //判断数据表是否存在，如果不存在则从topic首位置消费，并新建该表；
		//如果表存在，则从表中恢复话题对应分区的消息的offset
        boolean isExists = admin.tableExists(tn);
        System.out.println("HBase表:"+datatable+",是否存在:"+isExists);
        if (isExists)
        {
            try {
                HTable table = new HTable(conf, datatable);
                Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                        new BinaryComparator(Bytes.toBytes(topics+"_")));
                Scan s = new Scan();
                s.setFilter(filter);
                ResultScanner rs = table.getScanner(s);

                // begin from the the offsets committed to the database
                Map<TopicPartition, Long> fromOffsets = new HashMap<>();
                String s1 = null;  int s2 = 0;  long s3 = 0;
                for (Result r : rs) {
                    System.out.println("Rowkey:" + new String(r.getRow()));
                    for (KeyValue keyValue : r.raw()) {
                        if (new String(keyValue.getQualifier()).equals("topic"))
                        {
                            s1 = Bytes.toString(keyValue.getValue());
                            System.out.println("列族:" + new String(keyValue.getFamily()) + " 列:" + new String(keyValue.getQualifier()) + ":" + s1);
                        }

                        if (new String(keyValue.getQualifier()).equals("partitionnum"))
                        {
                            s2 = Bytes.toInt(keyValue.getValue());
                            System.out.println("列族:" + new String(keyValue.getFamily()) + " 列:" + new String(keyValue.getQualifier()) + ":" + s2);
                        }

                        if (new String(keyValue.getQualifier()).equals("partitionoffset"))
                        {
                            s3 = Bytes.toLong(keyValue.getValue());
                            System.out.println("列族:" + new String(keyValue.getFamily()) + " 列:" + new String(keyValue.getQualifier()) + ":" + s3);
                        }
                    }
                    fromOffsets.put(new TopicPartition(s1, s2), s3);
                }



                stream = KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Assign(fromOffsets.keySet(), kafkaParams, fromOffsets));

            }catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else
        {
            //如果不存在TopicOffset表，则从topic首位置开始消费
            stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams));

            //并创建TopicOffset表
            HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(datatable));
            hbaseTable.addFamily(new HColumnDescriptor("topic_partition_offset"));
            admin.createTable(hbaseTable);
            System.out.println(datatable + "表已经成功创建!----------------");
        }


        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {

                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                HTable table = new HTable(conf, datatable);
                Put put =null;
                for (OffsetRange offsetRange : offsetRanges) {
                    System.out.println("the topic is " + offsetRange.topic()+",\t"+"the partition is " + offsetRange.partition()
                            +",\t"+"the fromOffset is " + offsetRange.fromOffset()
                            +",\t"+ "the untilOffset is " + offsetRange.untilOffset()
                            +",\t"+"the object is " + offsetRange.toString());
                    // begin your transaction


                    // 为了保证业务的事务性，最好把业务计算结果和offset同时进行hbase的存储，这样可以保证要么都成功，要么都失败，最终从端到端体现消费精确一次消费的意境
                    // update results
                    // update offsets where the end of existing offsets matches the beginning of this batch of offsets
                    // assert that offsets were updated correctly
                    put = new Put(Bytes.toBytes(offsetRange.topic() + "_" + offsetRange.partition()));
                    put.add(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("topic"),
                            Bytes.toBytes(offsetRange.topic()));
                    put.add(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("partitionnum"),
                            Bytes.toBytes(offsetRange.partition()));
                    put.add(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("partitionoffset"),
                            Bytes.toBytes(offsetRange.untilOffset()));
                    table.put(put);
                    // end your transaction
                }
                System.out.println("Update offsets to HBase, is successful.");
                System.out.println("--------------The RDD records counts is " + rdd.count()+"----------------------");
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
