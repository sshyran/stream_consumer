require 'spec_helper'
require 'http_streaming_client'
require 'timeout'

require 'stream_consumer/updater/mysql_stats_updater'
require 'stream_consumer/producer/kafka_data_producer'

describe StreamConsumer::Consumer do

  describe "single consumer thread, single producer thread, console producer, console updater" do

    it "should successfully retrieve JSON records from the firehose" do

      expect {

	updater = StreamConsumer::Updater::MysqlStatsUpdater.new(config[:kafka][:topic_name], config[:database])
	producer = StreamConsumer::Producer::KafkaDataProducer.new(config[:num_producer_threads], config[:kafka][:topic_name], config[:kafka][:client_id], config[:kafka][:brokers])

	config[:run_id] = config[:kafka][:topic_name]
	options = { stats_updater: updater, data_producer: producer, options_factory: HttpStreamingClientOptions.new }
        consumer = StreamConsumer::Consumer.new(config.merge(options))

	begin
	  status = Timeout::timeout(TIMEOUT_SEC) {
	    consumer.run { |line,now| calculate_lag(line, now) }
	  }
	rescue Timeout::Error
	  logger.debug "Timeout occurred, #{TIMEOUT_SEC} seconds elapsed"
	  consumer.halt
	end
      }.to_not raise_error

    end

  end

end
