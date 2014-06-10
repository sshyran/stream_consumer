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
