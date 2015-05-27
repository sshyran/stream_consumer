###########################################################################
##
## stream_consumer
##
## Multi-threaded HTTP stream consumer, with asynchronous spooling
## and multi-threaded production.
##
## David Tompkins -- 05/30/2013
## tompkins@adobe_dot_com
##
###########################################################################
##
## Copyright (c) 2013 Adobe Systems Incorporated. All rights reserved.
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##
###########################################################################

require 'poseidon'

module StreamConsumer
  module Producer

    class KafkaDataProducer < DataProducer

      SOCKET_TIMEOUT_MS = 10000

      def initialize(num_producer_threads, topic_name, client_id, broker_array)
	@producers = (1..num_producer_threads).map { |i| ProducerCacheEntry.new(nil) }
	@topic_name = topic_name
	@client_id = client_id
	@broker_array = broker_array
      end

      def format(line)
	Poseidon::MessageToSend.new(@topic_name, line)
      end

      def produce(thread_id, messages)
	get_topic_producer(thread_id, @topic_name).send_messages(messages)
	logger.debug "Messages produced to kafka: thread #{thread_id}"
      end

      protected

      @@broker_pool = nil

      def get_kafka_broker_pool
	return @@broker_pool if !@@broker_pool.nil?
	begin
	  @@broker_pool = Poseidon::BrokerPool.new(@client_id, @broker_array.map { |broker| "#{broker[:hostname]}:#{broker[:port]}" }, SOCKET_TIMEOUT_MS)
	  return @@broker_pool
	rescue Exception => e
	  logger.error "get_kafka_broker_pool:exception creating broker pool:#{e}"
	  return nil
	end
      end

      def get_kafka_metadata(broker_pool, topic_name)
	metadata = broker_pool.fetch_metadata([topic_name])
	logger.debug "kafka metadata: #{metadata.to_yaml}"

	brokers = Hash.new
	metadata[:brokers].each do |broker|
	  brokers[broker.id] = { host: broker.host, port: broker.port, connect: "#{broker.host}:#{broker.port}" }
	end

	topics = Array.new
	metadata[:topics].each do |topic|
	  topics << { name: topic.name, leader: topic.partitions[0].leader, replicas: topic.partitions[0].replicas, isr: topic.partitions[0].isr }
	end

	return brokers, topics
      end

      def get_topic_producer(thread_id, topic_name)
	return @producers[thread_id - 1].producer unless @producers[thread_id - 1].is_expired?

	if !@producers[thread_id - 1].producer.nil? then
	  logger.info "producer expired for thread id #{thread_id}, reacquiring kafka metadata for topic #{topic_name}"
	  @producers[thread_id - 1].producer.shutdown
	end

	broker_pool = get_kafka_broker_pool
	brokers, topics = get_kafka_metadata(broker_pool, topic_name)
	leader = brokers[topics[0][:leader]]
	logger.info "Connecting producer to #{leader[:host]}:#{leader[:port]} for #{topic_name}, thread id #{thread_id}"
	@producers[thread_id - 1] = ProducerCacheEntry.new(Poseidon::Producer.new(["#{leader[:host]}:#{leader[:port]}"], @client_id, :type => :sync, :compression_codec => :gzip, :metadata_refresh_interval_ms => 120000))
	return @producers[thread_id - 1].producer
      end

    end

  end
end
