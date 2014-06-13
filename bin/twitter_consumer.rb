#!/usr/bin/env ruby

require 'logger'
require 'http_streaming_client'
require 'stream_consumer'
require 'stream_consumer/updater/mysql_stats_updater'
require 'stream_consumer/producer/kafka_data_producer'

require '/opt/rails/bluejay/shared/credentials/credentials'
include HttpStreamingClient::Credentials::Twitter

def config
  StreamConsumer::Config.instance("config/twitter_consumer.conf")
end

def logger
  StreamConsumer.logger
end

StreamConsumer.logger.logfile = config[:logfile]
StreamConsumer.logger.console = false
StreamConsumer.logger.level = Logger::INFO
StreamConsumer.logger.tag = config[:logtag]

updater = StreamConsumer::Updater::MysqlStatsUpdater.new(config[:kafka][:topic_name], config[:database])
producer = StreamConsumer::Producer::KafkaDataProducer.new(config[:num_producer_threads], config[:kafka][:topic_name], config[:kafka][:client_id], config[:kafka][:brokers])

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

options = { stats_updater: updater, data_producer: producer, options_factory: HttpStreamingClientOptions.new }
consumer = StreamConsumer::Consumer.new(config.merge(options))

Signal.trap("TERM") do
  logger.warn "SIGTERM received, halting..."
  consumer.halt
end

Signal.trap("INT") do
  logger.warn "SIGINT received, halting..."
  consumer.halt
end

consumer.run { |line,now| calculate_lag(line, now) }
