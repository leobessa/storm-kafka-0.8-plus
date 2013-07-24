package storm.kafka.trident;

import backtype.storm.metric.api.IMetric;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class KafkaUtils {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

	public static IBrokerReader makeBrokerReader(Map stormConf, KafkaConfig conf) {
		if(conf.hosts instanceof StaticHosts) {
			return new StaticBrokerReader(((StaticHosts) conf.hosts).getPartitionInformation());
		} else {
			return new ZkBrokerReader(stormConf, conf.topic, (ZkHosts) conf.hosts);
		}
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		OffsetRequest request = new OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);

        final OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()){
            LOG.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
	}

	public static class KafkaOffsetMetric implements IMetric {
        Map<GlobalPartitionId, Long> _partitionToOffset = new HashMap<GlobalPartitionId, Long>();
        Set<GlobalPartitionId> _partitions;
        String _topic;
        DynamicPartitionConnections _connections;

        public KafkaOffsetMetric(String topic, DynamicPartitionConnections connections) {
            _topic = topic;
            _connections = connections;
        }

        public void setLatestEmittedOffset(GlobalPartitionId partition, long offset) {
            _partitionToOffset.put(partition, offset);
        }

        @Override
        public Object getValueAndReset() {
            try {
                long totalSpoutLag = 0;
                long totalLatestTimeOffset = 0;
                long totalLatestEmittedOffset = 0;
                HashMap ret = new HashMap();
                if(_partitions != null && _partitions.size() == _partitionToOffset.size()) {
                    for(Map.Entry<GlobalPartitionId, Long> e : _partitionToOffset.entrySet()) {
                        GlobalPartitionId partition = e.getKey();
                        SimpleConsumer consumer = _connections.getConnection(partition);
                        if(consumer == null) {
                            LOG.warn("partitionToOffset contains partition not found in _connections. Stale partition data?");
                            return null;
                        }
                        long latestTimeOffset = getLastOffset(consumer, _topic, partition.partition, kafka.api.OffsetRequest.LatestTime(), consumer.clientId());
                        if(latestTimeOffset == 0) {
                            LOG.warn("No data found in Kafka Partition " + partition.getId());
                            return null;
                        }
                        long latestEmittedOffset = e.getValue();
                        long spoutLag = latestTimeOffset - latestEmittedOffset;
                        ret.put(partition.getId() + "/" + "spoutLag", spoutLag);
                        ret.put(partition.getId() + "/" + "latestTime", latestTimeOffset);
                        ret.put(partition.getId() + "/" + "latestEmittedOffset", latestEmittedOffset);
                        totalSpoutLag += spoutLag;
                        totalLatestTimeOffset += latestTimeOffset;
                        totalLatestEmittedOffset += latestEmittedOffset;
                    }
                    ret.put("totalSpoutLag", totalSpoutLag);
                    ret.put("totalLatestTime", totalLatestTimeOffset);
                    ret.put("totalLatestEmittedOffset", totalLatestEmittedOffset);
                    return ret;
                } else {
                    LOG.info("Metrics Tick: Not enough data to calculate spout lag.");
                }
            } catch(Throwable t) {
                LOG.warn("Metrics Tick: Exception when computing kafkaOffset metric.", t);
            }
            return null;
        }

       public void refreshPartitions(Set<GlobalPartitionId> partitions) {
           _partitions = partitions;
           Iterator<GlobalPartitionId> it = _partitionToOffset.keySet().iterator();
           while(it.hasNext()) {
               if(!partitions.contains(it.next())) it.remove();
           }
       }
    };
}
