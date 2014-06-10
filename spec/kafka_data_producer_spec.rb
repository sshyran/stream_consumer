require 'spec_helper'

require 'stream_consumer/producer/kafka_data_producer'

describe StreamConsumer::Producer::KafkaDataProducer do

  describe "test kafka data producer" do

    it "should successfully connect to kafka and produce a message" do

      expect {
	producer = StreamConsumer::Producer::KafkaDataProducer.new(config[:num_producer_threads], config[:kafka][:topic_name], config[:kafka][:client_id], config[:kafka][:brokers])
	messages = Array.new
	messages << producer.format("Test Message")
	producer.produce(1, messages)
      }.to_not raise_error

    end

  end

end
