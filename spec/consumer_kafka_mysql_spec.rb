require 'spec_helper'
require 'http_streaming_client'
require 'timeout'

require 'stream_consumer/updater/mysql_stats_updater'
require 'stream_consumer/producer/kafka_data_producer'

describe StreamConsumer::Consumer do

  describe "single consumer thread, single producer thread, console producer, console updater" do

    it "should successfully retrieve JSON records from the firehose" do

      expect {

	updater = StreamConsumer::Updater::MysqlStatsUpdater.new(TOPIC_NAME, DATABASE_CONFIG)
	producer = StreamConsumer::Producer::KafkaDataProducer.new(NUM_PRODUCER_THREADS, TOPIC_NAME, CLIENT_ID, KAFKA_BROKER_ARRAY)

	options = { num_producer_threads: NUM_PRODUCER_THREADS, num_consumer_threads: NUM_CONSUMER_THREADS, client_id: CLIENT_ID, stats_updater: updater, data_producer: producer }
	consumer = StreamConsumer::Consumer.new(options)

	run_options = { url: "#{STREAMURL}?stall_warnings=true", options_factory: HttpStreamingClientOptions.new, run_id: TOPIC_NAME, records_per_batch: 100, min_batch_seconds: 20, signal_prefix_array: SIGNAL_PREFIX_ARRAY, reconnect: false }

	begin
	  status = Timeout::timeout(TIMEOUT_SEC) {
	    consumer.run(run_options) { |line,now| calculate_lag(line, now) }
	  }
	rescue Timeout::Error
	  logger.debug "Timeout occurred, #{TIMEOUT_SEC} seconds elapsed"
	  consumer.halt
	end
      }.to_not raise_error

    end

  end

end
