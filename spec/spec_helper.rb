require 'bundler/setup'
require 'logger'

require 'coveralls'
Coveralls.wear! if ENV["COVERALLS"]

require 'simplecov'
SimpleCov.start do
  add_filter "/spec/"
end

require '/opt/rails/bluejay/shared/credentials/credentials'
include HttpStreamingClient::Credentials::Twitter

require 'stream_consumer'

def config
  StreamConsumer::Config.instance("config/stream_consumer_rspec.conf")
end

def logger
  StreamConsumer.logger
end

RSpec.configure do |conf|
  StreamConsumer.logger.logfile = config[:logfile]
  StreamConsumer.logger.console = true
  StreamConsumer.logger.level = Logger::INFO
  StreamConsumer.logger.tag = config[:logtag]
  conf.filter_run_excluding disabled: true
end

TIMEOUT_SEC = 75
CHECKPOINT_DATA = { :tag=>config[:kafka][:client_id], :messages_consumed_per_sec=>40.0, :messages_produced_per_sec=>40.0, :kbytes_consumed_per_sec=>103.29, :lag=>-1.0 } 

def calculate_lag(line, now)
  # calculate message arrival lag
  # expect records to start with something like this: {"created_at":"Sat Jan 11 00:19:26 +0000 2014",
  lag = -1
  if line.start_with? config[:date_prefix] then
    begin
      record_time = DateTime.parse(line[config[:date_prefix].length, config[:date_format].length]).to_time
      lag = now - record_time
      logger.debug "#{config[:kafka][:topic_name]}:message lag calculator:lag:#{lag}"
    rescue Exception => e
      logger.error "#{config[:kafka][:topic_name]}:lag calculation:exception:#{e}"
    end
  else
    logger.debug "#{config[:kafka][:topic_name]}:message lag calculator:message 'created_at:' field not found, lag unknown"
  end
  lag
end

# Create http_streaming_client option/header factory for authorization header OAuth Bearer Tokens
class HttpStreamingClientOptions
  def get_options
    authorization = HttpStreamingClient::Oauth::Twitter.generate_authorization(config.stream_url, "get", config.stream_params, OAUTH_CONSUMER_KEY, OAUTH_CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    { :headers => {'Authorization' => "#{authorization}" } }
  end
end
