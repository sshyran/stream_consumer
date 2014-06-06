require 'bundler/setup'
require 'logger'

require 'coveralls'
Coveralls.wear! if ENV["COVERALLS"]

require 'simplecov'
SimpleCov.start do
  add_filter "/spec/"
end

require 'stream_consumer'

require '/opt/rails/bluejay/shared/credentials/credentials'
include HttpStreamingClient::Credentials::Twitter

TOPIC_NAME="stream_consumer_test"
NUM_PRODUCER_THREADS = 1
NUM_CONSUMER_THREADS = 1
CLIENT_ID = "stream_consumer_rspec"
TIMEOUT_SEC = 75

VALID_RECORD_PREFIX = '{"created_at":"'
DATE_FORMAT_SAMPLE = 'Sat Jan 11 00:19:26 +0000 2014'
SIGNAL_PREFIX_ARRAY = [ '{"warning":', '{"disconnect":' ]

STREAMURL = "https://stream.twitter.com/1.1/statuses/sample.json"
KAFKA_BROKER_ARRAY = [ {hostname: "fenway.corp.adobe.com", port: 9092}, {hostname: "wrigley.corp.adobe.com", port: 9092}, {hostname: "candlestick.corp.adobe.com", port: 9092}]
DATABASE_CONFIG = { "encoding" => "utf8", "database" => "bluejay_development", "pool" => 5, "username" => "bluejay_user", "password" => "bluejay123", "host" => "localhost", "port" => 3306 }
CHECKPOINT_DATA = { :tag=>CLIENT_ID, :messages_consumed_per_sec=>40.0, :messages_produced_per_sec=>40.0, :kbytes_consumed_per_sec=>103.29, :lag=>-1.0 } 

RSpec.configure do |config|

  StreamConsumer.logger.console = true
  StreamConsumer.logger.level = Logger::INFO
  StreamConsumer.logger.logfile = true
  StreamConsumer.logger.tag = "rspec"

  config.filter_run_excluding disabled: true

end

def logger
  StreamConsumer.logger
end

def calculate_lag(line, now)
  # calculate message arrival lag
  # expect records to start with something like this: {"created_at":"Sat Jan 11 00:19:26 +0000 2014",
  lag = -1
  if line.start_with? VALID_RECORD_PREFIX then
    begin
      record_time = DateTime.parse(line[VALID_RECORD_PREFIX.length, DATE_FORMAT_SAMPLE.length]).to_time
      lag = now - record_time
      logger.debug "#{TOPIC_NAME}:message lag calculator:lag:#{lag}"
    rescue Exception => e
      logger.error "#{TOPIC_NAME}:lag calculation:exception:#{e}"
    end
  else
    logger.debug "#{TOPIC_NAME}:message lag calculator:message 'created_at:' field not found, lag unknown"
  end
  lag
end

# Create http_streaming_client option/header factory for authorization header OAuth Bearer Tokens
class HttpStreamingClientOptions
  def get_options
    authorization = HttpStreamingClient::Oauth::Twitter.generate_authorization(STREAMURL, "get", { 'stall_warnings' => true }, OAUTH_CONSUMER_KEY, OAUTH_CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    { :headers => {'Authorization' => "#{authorization}" } }
  end
end
