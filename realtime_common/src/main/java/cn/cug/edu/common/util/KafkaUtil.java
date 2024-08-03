package cn.cug.edu.common.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

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

}
