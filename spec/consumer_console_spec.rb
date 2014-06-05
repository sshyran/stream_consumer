require 'spec_helper'
require 'http_streaming_client'
require 'timeout'

require 'stream_consumer/updater/console_stats_updater'
require 'stream_consumer/producer/console_data_producer'

require '/opt/rails/bluejay/shared/credentials/credentials'
include HttpStreamingClient::Credentials::Twitter

STREAMURL = "https://stream.twitter.com/1.1/statuses/sample.json"
# Create http_streaming_client option/header factory for authorization header OAuth Bearer Tokens
class HttpStreamingClientOptions
  def get_options
    authorization = HttpStreamingClient::Oauth::Twitter.generate_authorization(STREAMURL, "get", { 'stall_warnings' => true }, OAUTH_CONSUMER_KEY, OAUTH_CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    { :headers => {'Authorization' => "#{authorization}" } }
  end
end

VALID_RECORD_PREFIX = '{"created_at":"'
DATE_FORMAT_SAMPLE = 'Sat Jan 11 00:19:26 +0000 2014'
SIGNAL_PREFIX_ARRAY = [ '{"warning":', '{"disconnect":' ]
TOPIC_NAME="twitter_firehose"
TIMEOUT_SEC = 75

updater = StreamConsumer::Updater::ConsoleStatsUpdater.new
producer = StreamConsumer::Producer::ConsoleDataProducer.new

describe StreamConsumer::Consumer do

  describe "single consumer thread, single producer thread, console producer, console updater" do

    it "should successfully retrieve JSON records from the firehose" do

      expect {
	options = { num_producer_threads: 1, num_consumer_threads: 1, client_id: "bluejay_consumer_twitter", stats_updater: updater, data_producer: producer }
	consumer = StreamConsumer::Consumer.new(options)

	run_options = { url: "#{STREAMURL}?stall_warnings=true", options_factory: HttpStreamingClientOptions.new, run_id: TOPIC_NAME, records_per_batch: 100, min_batch_seconds: 20, signal_prefix_array: SIGNAL_PREFIX_ARRAY, reconnect: false }

	begin
	  status = Timeout::timeout(TIMEOUT_SEC) {

	    consumer.run(run_options) { |line,now|
	    # calculate message arrival lag
	    # expect records to start with something like this: {"created_at":"Sat Jan 11 00:19:26 +0000 2014",
	    lag = -1
	    if line.start_with? VALID_RECORD_PREFIX then
	      begin
		record_time = DateTime.parse(line[VALID_RECORD_PREFIX.length, DATE_FORMAT_SAMPLE.length]).to_time
		lag = now - record_time
		consumer.logger.debug "#{TOPIC_NAME}:message lag calculator:lag:#{lag}"
	      rescue Exception => e
		consumer.logger.error "#{TOPIC_NAME}:lag calculation:exception:#{e}"
	      end
	    else
	      consumer.logger.debug "#{TOPIC_NAME}:message lag calculator:message 'created_at:' field not found, lag unknown"
	    end
	    lag
	  }
	  }
	rescue Timeout::Error
	  logger.debug "Timeout occurred, #{TIMEOUT_SEC} seconds elapsed"
	  consumer.halt
	end
      }.to_not raise_error

    end

  end

end
