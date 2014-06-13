#!/usr/bin/env ruby

require 'logger'
require 'http_streaming_client'
require 'stream_consumer'
require 'stream_consumer/updater/mysql_stats_updater'
require 'stream_consumer/producer/kafka_data_producer'

require '/opt/rails/bluejay/shared/credentials/credentials'
include HttpStreamingClient::Credentials::Adobe

def config
  StreamConsumer::Config.instance("config/adobe_consumer.conf")
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
  # expect records to start with something like this:
  # {"reportSuite":"mx-geo-nz","service":"ss","timeGMT":1390776457,"receivedTimeGMT":1390776457,
  # old format:
  # {"reportSuite"=>"mx-geo-nz", "service"=>"ss", "timeGMT"=>1390772908, "receivedTimeGMT"=>1390772908,
  lag = -1
  if line.start_with? config[:date_prefix] then
    begin
      index = line.index('receivedTimeGMT')
      if !index.nil? then
	epoch_time_start = line.index(':', index)
	if !epoch_time_start.nil? then
	  epoch_time_end = line.index(',', epoch_time_start)
	  if !epoch_time_end.nil? then
	    epoch_time_utc = line[(epoch_time_start+1)..(epoch_time_end-1)].to_i
	    if epoch_time_utc > 0 then
	      lag = now.to_i - epoch_time_utc
	      consumer.logger.debug "#{config[:kafka][:topic_name]}:message lag calculator:lag:#{lag}"
	    else
	      consumer.logger.debug "#{config[:kafka][:topic_name]}:message lag calculator:error parsing 'receivedTimeGMT', substring parse error: #{line[(epoch_time_start+1)..(epoch_time_end-1)]}"
	    end
	  else
	    consumer.logger.debug "#{config[:kafka][:topic_name]}:message lag calculator:error parsing 'receivedTimeGMT', ',' not found, lag unknown"
	  end
	else
	  consumer.logger.debug "#{config[:kafka][:topic_name]}:message lag calculator:error parsing 'receivedTimeGMT', '>' not found, lag unknown"
	end
      else
	consumer.logger.debug "#{config[:kafka][:topic_name]}:message lag calculator:message 'receivedTimeGMT' field not found, lag unknown"
      end
    rescue Exception => e
      consumer.logger.error "#{config[:kafka][:topic_name]}:lag calculation:exception:#{e}"
    end
  else
    consumer.logger.debug "#{config[:kafka][:topic_name]}:message lag calculator:message 'reportSuite' field not found, lag unknown"
  end
  lag
end

# Create http_streaming_client option/header factory for authorization header OAuth Bearer Tokens
class HttpStreamingClientOptions
  def get_options
    authorization = HttpStreamingClient::Oauth::Adobe.generate_authorization(TOKENAPIHOST, USERNAME, PASSWORD, CLIENTID, CLIENTSECRET)
    { :headers => {'Authorization' => "Bearer #{authorization}" } }
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
