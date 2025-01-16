package cn.cug.edu.common.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * author song
 * date 2024-08-03 21:03
 * Desc
 */
public class KafkaUtil {

    //获取kafkasource
    public static KafkaSource<String> getKfSource(String groupId,String topic){

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                //kafka集群地址 从配置文件中读取
                .setBootstrapServers(ConfigUtil.getString("KAFKA_BROKERS"))
                //kafkasource作为消费者 需要消费者组id 消费者组id每个job 不一样所以需要参数传递
                .setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                //设置需要消费的主题 topic应该也是不一样的
                .setTopics(topic)
                //设置value的反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //设置消费策略 消费策略这一块 如果开启了ck的话 就不需要设置了 但是了为调试方便 设置成从头开始消费
                .setStartingOffsets(OffsetsInitializer.earliest())
                //设置事务隔离级别  读已提交
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        return kafkaSource;


    }

    // 定义一个静态方法 实现返回kafkasink的方法
    public static KafkaSink<String> getKfSink(String topic){

        //作为kafkasink表示的是一个生产者 就需要一个topic 以及连接地址 以及对value的序列化
        KafkaSink<String> kafkasink = KafkaSink.<String>builder()
                .setBootstrapServers(ConfigUtil.getString("KAFKA_BROKERS"))
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        //这里的topic 不是固定的 需要参数传递进来
                        .setTopic(topic)
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("song-" + topic)
                .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1000")
                .setProperty(ProducerConfig.LINGER_MS_CONFIG,"1000")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();

        return kafkasink;

    }

}
