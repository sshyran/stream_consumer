###########################################################################
##
## stream_consumer
##
## Multi-threaded HTTP stream consumer, with asynchronous spooling
## and multi-threaded production.
##
## David Tompkins -- 05/30/2013
## tompkins@adobe_dot_com
##
###########################################################################
##
## Copyright (c) 2013 Adobe Systems Incorporated. All rights reserved.
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##
###########################################################################

require 'yaml'
require 'logger'
require 'http_streaming_client'
require 'poseidon'

module StreamConsumer

  class Consumer

    include Loggable

    # options:
    # :num_producer_threads
    # :num_consumer_threads
    # :client_id
    # :database_config
    def initialize(options, &stats_checkpoint_callback)
      
      HttpStreamingClient.logger = logger

      @options = options

      if !@options[:database_config].nil? then
	logger.info "Database configuration: #{@options[:database_config]}"
	@database_adapter = MySqlAdapter.new(@options[:database_config], logger)
      else
	@database_adapter = nil
      end

      @production_queue = Queue.new
      @producers = (1..@options[:num_producer_threads]).map { |i| ProducerCacheEntry.new(nil) }
      @producer_threads = (1..@options[:num_producer_threads]).map { |i| Thread.new(i) { |ii| produce_messages(ii) } }

      @stats = Stats.new(@options[:client_id])
      @stats.run { |checkpoint| stats_checkpoint_callback(checkpoint, @database_adapter) unless stats_checkpoint_callback.nil? }

      @running = true

    end

    def halt
      @running = false
    end

    def produce_messages(thread_id)
      begin
	while @running 
	  job = @production_queue.pop
	  logger.info "Production job accepted: thread #{thread_id}, job id #{job.id}, topic #{job.topic_name}"

	  now = Time.new
	  logger.debug "Message stats recorded: thread #{thread_id}"

	  get_topic_producer(thread_id, job.topic_name).send_messages(job.messages)
	  logger.debug "Messages produced to kafka: thread #{thread_id}"

	  @stats.add_produced(job.messages.size)

	  topic_name = job.topic_name
	  size = job.messages.size
	  id = job.id
	  job.clear
	  job = nil
	  elapsed_time = Time.new - now
	  logger.info "#{size} messages produced to topic: #{topic_name}, job id #{id}, production elapsed time: #{elapsed_time} seconds"
	  topic_name = nil
	end
      rescue Exception => e
	logger.error "Production job thread #{thread_id}:Exception:#{e}"
      end
    end

    # options:
    # :url
    # :options_factory
    # :topic_name
    # :records_per_batch
    # :min_batch_seconds
    # :signal_prefix_array
    # :reconnect
    # :reconnect_interval
    # :reconnect_attempts
    def run(run_options, &block)

      @run_options = run_options
      @topic_name = run_options[:topic_name]
      @records_per_batch = run_options[:records_per_batch]
      @min_batch_seconds = run_options[:min_batch_seconds]

      @inbound = get_inbound(run_options[:topic_name])

      logger.info "-----------------------------------------------------------------"
      logger.info "url: #{url}"
      logger.info "topic: #{topic_name}"
      logger.info "inbound id: #{@inbound[:inbound_id]}"
      logger.info "consumer threads: #{@num_consumer_threads}"
      logger.info "producer threads: #{@num_producer_threads}"
      logger.info "-----------------------------------------------------------------"

      @consumer_threads = (1..@options[:num_consumer_threads]).map { |i| Thread.new(i) { |ii| consume_messages(ii, &block) } }

      @consumer_threads.each { |thread| thread.join }
      @producer_threads.each { |thread| thread.exit }
      @stats.halt

    end

    def consume_messages(thread_id, &block)
      begin
	logger.info "starting consumer #{thread_id} for topic #{@topic_name}: #{Time.new.to_s}"

	# Prepare for batch run
	totalCount = 0
	intervalCount = 0
	intervalSize = 0
	startTime = nil
	lastTime = nil
	messages = []

	if @run_options[:reconnect] then
	  client = HttpStreamingClient::Client.new(compression: true, reconnect: true, reconnect_interval: @run_options[:reconnect_interval], reconnect_attempts: @run_options[:reconnect_attempts])
	else
	  client = HttpStreamingClient::Client.new(compression: true)
	end

	startTime = lastTime = Time.new 

	response = client.get(@run_options[:url], { :options_factory => @run_options[:options_factory] }) { |line|

	  if line.nil? then
	    logger.info "consumer #{thread_id}:error:nil line received for topic: #{@topic_name}"
	    next
	  end

	  if line.size == 0 then
	    logger.info "consumer #{thread_id}:error:zero length line received for topic: #{@topic_name}"
	    next
	  end

	  if line.eql? "\r\n" then
	    logger.info "consumer #{thread_id}:Server ping received for topic: #{@topic_name}"
	    next
	  end

	  if !@run_options[:signal_prefix_array].nil? then
	    signal_message_received = false
	    @run_options[:signal_prefix_array].each do |prefix|
	      if line.start_with? prefix then
		logger.info "consumer #{thread_id}:Stream signal message received for topic:#{@topic_name}:#{line}"
		signal_message_received = true
		break
	      end
	    end
	    next if signal_message_received
	  end

	  logger.debug "consumer #{thread_id}:line: #{line}"

	  messages << Poseidon::MessageToSend.new(@topic_name, line)

	  totalCount = totalCount + 1
	  intervalCount = intervalCount + 1
	  intervalSize = intervalSize + line.size

	  if (totalCount % @records_per_batch) == 0 then

	    now = Time.new
	    logger.debug "consumer #{thread_id}:next because < @min_batch_seconds" if (now - lastTime) < @min_batch_seconds
	    next if (now - lastTime) < @min_batch_seconds

	    lag = -1 # lag unknown
	    if block_given? then
	      # block is code to detect lag
	      lag = block.call(line, now)
	      logger.debug "consumer #{thread_id}:lag handler called, calculated lag: #{lag}"
	    end

	    @stats.add_consumed(intervalCount, intervalSize, lag)

	    intervalElapsedTime = now - lastTime

	    message_set_params = { inbound_id: @inbound[:inbound_id], num_records: intervalCount, size_bytes: intervalSize, time_sec: intervalElapsedTime.round(2).to_s, records_per_sec: (intervalCount / intervalElapsedTime).round(2).to_s, kbytes_per_sec: (intervalSize / intervalElapsedTime / 1024).round(2).to_s, message_lag: lag }

	    production_job = ProductionJob.new(@topic_name, messages, message_set_params)
	    @production_queue << production_job
	    logger.info "consumer #{thread_id}:production job queued, #{messages.size} messages for #{@topic_name}, job id #{production_job.id}, queue length #{@production_queue.length}"

	    production_job = nil
	    messages = []
	    lastTime = Time.new
	    intervalSize = 0
	    intervalCount = 0
	  end

	  if !@running then

	    logger.info "consumer #{thread_id}:shutting down, interrupted: #{Time.new.to_s}"

	    if messages.size > 0 then

	      now = Time.new

	      lag = -1 # lag unknown
	      if block_given? then
		# block is code to detect lag
		lag = block.call(line, now)
		logger.debug "consumer #{thread_id}:lag handler called, calculated lag: #{lag}"
	      end

	      @stats.add_consumed(intervalCount, intervalSize, lag)

	      intervalElapsedTime = now - lastTime

	      message_set_params = { inbound_id: @inbound[:inbound_id], num_records: intervalCount, size_bytes: intervalSize, time_sec: intervalElapsedTime.round(2).to_s, records_per_sec: (intervalCount / intervalElapsedTime).round(2).to_s, kbytes_per_sec: (intervalSize / intervalElapsedTime / 1024).round(2).to_s, message_lag: lag }
	      #write_inbound_stats(message_set_params)

	      broker_pool = get_kafka_broker_pool
	      brokers, topics = get_kafka_metadata(broker_pool, @topic_name)
	      leader = brokers[topics[0][:leader]]
	      producer = Poseidon::Producer.new(["#{leader[:hostname]}:#{leader[:port]}"], "#{@topic_name}")
	      producer.send_messages(messages)
	      logger.info "consumer #{thread_id}:#{intervalCount} messages produced to topic: #{@topic_name}"
	    end

	    logger.info "consumer #{thread_id}:shut down complete: #{Time.new.to_s}"
	    logger.info "consumer #{thread_id}:total elapsed time: #{(Time.new - startTime).round(2).to_s} seconds"
	    exit 0
	  end
	}
      rescue Exception => e
	logger.error "Consumer thread #{thread_id}:Exception:#{e}"
      end
    end

    protected

    @@broker_pool = nil

    def get_kafka_broker_pool
      return @@broker_pool if !@@broker_pool.nil?
      brokers = get_brokers
      begin
	@@broker_pool = Poseidon::BrokerPool.new(@client_id, brokers.map { |broker| "#{broker[:hostname]}:#{broker[:port]}" })
	return @@broker_pool
      rescue Exception => e
	logger.error "get_kafka_broker_pool:exception creating broker pool:#{e}"
	return nil
      end
    end

    def get_kafka_metadata(broker_pool, topic_name)
      metadata = broker_pool.fetch_metadata([topic_name])
      logger.debug "kafka metadata: #{metadata.to_yaml}"

      brokers = Hash.new
      metadata[:brokers].each do |broker|
	brokers[broker.id] = { host: broker.host, port: broker.port, connect: "#{broker.host}:#{broker.port}" }
      end

      topics = Array.new
      metadata[:topics].each do |topic|
	topics << { name: topic.name, leader: topic.partitions[0].leader, replicas: topic.partitions[0].replicas, isr: topic.partitions[0].isr }
      end

      return brokers, topics
    end

    def get_topic_producer(thread_id, topic_name)
      return @producers[thread_id - 1].producer unless @producers[thread_id - 1].is_expired?

      if !@producers[thread_id - 1].producer.nil? then
	logger.info "producer expired for thread id #{thread_id}, reacquiring kafka metadata for topic #{topic_name}"
	@producers[thread_id - 1].producer.shutdown
      end

      broker_pool = get_kafka_broker_pool
      brokers, topics = get_kafka_metadata(broker_pool, topic_name)
      leader = brokers[topics[0][:leader]]
      logger.info "Connecting producer to #{leader[:host]}:#{leader[:port]} for #{topic_name}, thread id #{thread_id}"
      @producers[thread_id - 1] = ProducerCacheEntry.new(Poseidon::Producer.new(["#{leader[:host]}:#{leader[:port]}"], @client_id, :type => :sync, :compression_codec => :gzip, :metadata_refresh_interval_ms => 120000))
      return @producers[thread_id - 1].producer
    end

    def get_inbound(topic_name)
      query = "select t1.id,t2.* from inbounds t1 inner join topics t2 where t2.name='#{topic_name}' and t1.id = t2.inbound_id;"
      @database_adapter.query(query).first
    end

    def get_broker(leader_id)
      query = "select * from brokers where broker_id='#{leader_id}'"
      @database_adapter.query(query).first
    end

    def get_brokers()
      query = "select * from brokers"
      @database_adapter.query(query)
    end

    def write_inbound_stats(params)
      query = "insert into `inbound_stats` (`created_at`, `inbound_id`, `kbytes_per_sec`, `num_records`, `records_per_sec`, `size_bytes`, `time_sec`, `message_lag`) values ('#{Time.now.utc.strftime('%Y-%m-%d %H:%M:%S')}', #{params[:inbound_id]}, #{params[:kbytes_per_sec]}, #{params[:num_records]}, #{params[:records_per_sec]}, #{params[:size_bytes]}, #{params[:time_sec]}, #{params[:message_lag]})"
      @database_adapter.update(query)
    end

    def write_inbound_stats_v2(params)
      query = "insert into `inbound_stats_v2` (`created_at`, `inbound_id`, `consumed_records_per_sec`, `consumed_kbytes_per_sec`, `produced_records_per_sec`, `message_lag`) values ('#{params[:timestamp].utc.strftime('%Y-%m-%d %H:%M:%S')}', #{@inbound[:inbound_id]}, #{params[:messages_consumed_per_sec]}, #{params[:kbytes_consumed_per_sec]}, #{params[:messages_produced_per_sec]}, #{params[:lag]})"
      @database_adapter.update(query)
    end

  end

end
