require 'spec_helper'
require 'http_streaming_client'
require 'timeout'

require 'stream_consumer/updater/console_stats_updater'
require 'stream_consumer/producer/console_data_producer'

describe StreamConsumer::Consumer do

  describe "single consumer thread, single producer thread, console producer, console updater" do

    it "should successfully retrieve JSON records from the firehose" do

      expect {
	updater = StreamConsumer::Updater::ConsoleStatsUpdater.new
	producer = StreamConsumer::Producer::ConsoleDataProducer.new
	options = { num_producer_threads: 1, num_consumer_threads: 1, client_id: "bluejay_consumer_twitter", stats_updater: updater, data_producer: producer }
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
