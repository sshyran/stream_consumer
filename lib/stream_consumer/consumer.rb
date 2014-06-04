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

require 'http_streaming_client'

module StreamConsumer

  class Consumer

    include Loggable

    # options:
    # :num_producer_threads
    # :num_consumer_threads
    # :client_id
    # :stats_updater
    # :data_producer
    def initialize(options)
      
      HttpStreamingClient.logger = logger

      @options = options

      @production_queue = Queue.new
      @producer_threads = (1..@options[:num_producer_threads]).map { |i| Thread.new(i) { |thread_id| produce_messages(thread_id) } }

      @stats = Stats.new(@options[:client_id])
      @stats.start { |checkpoint| @options[:stats_updater].update(checkpoint) unless @options[:stats_updater].nil? }

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

	  @options[:data_producer].produce(thread_id, job.messages) unless @options[:data_producer].nil?
	  @stats.add_produced(job.messages.size)

	  msg = "#{job.messages.size} messages produced to topic: #{job.topic_name}, job id #{job.id}"
	  job.clear
	  job = nil
	  elapsed_time = Time.new - now
	  logger.info "#{msg}, production elapsed time: #{elapsed_time} seconds"
	end
      rescue Exception => e
	logger.error "Production job thread #{thread_id}:Exception:#{e}"
      end
    end

    # options:
    # :url
    # :options_factory
    # :run_id
    # :records_per_batch
    # :min_batch_seconds
    # :signal_prefix_array
    # :reconnect
    # :reconnect_interval
    # :reconnect_attempts
    def run(run_options, &block)

      @run_options = run_options
      @run_id = run_options[:run_id]
      @records_per_batch = run_options[:records_per_batch]
      @min_batch_seconds = run_options[:min_batch_seconds]

      logger.info "-----------------------------------------------------------------"
      logger.info "url: #{run_options[:url]}"
      logger.info "run_id: #{@run_id}"
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
	logger.info "starting consumer #{thread_id} for id #{@run_id}: #{Time.new.to_s}"

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
	    logger.info "consumer #{thread_id}:error:nil line received for id: #{@run_id}"
	    next
	  end

	  if line.size == 0 then
	    logger.info "consumer #{thread_id}:error:zero length line received for id: #{@run_id}"
	    next
	  end

	  if line.eql? "\r\n" then
	    logger.info "consumer #{thread_id}:Server ping received for id: #{@run_id}"
	    next
	  end

	  if !@run_options[:signal_prefix_array].nil? then
	    signal_message_received = false
	    @run_options[:signal_prefix_array].each do |prefix|
	      if line.start_with? prefix then
		logger.info "consumer #{thread_id}:Stream signal message received for id:#{@run_id}:#{line}"
		signal_message_received = true
		break
	      end
	    end
	    next if signal_message_received
	  end

	  logger.debug "consumer #{thread_id}:line: #{line}"

	  messages << @options[:data_producer].format(line) unless @options[:data_producer].nil?

	  totalCount = totalCount + 1
	  intervalCount = intervalCount + 1
	  intervalSize = intervalSize + line.size

	  if (totalCount % @records_per_batch) == 0 or !@running then
	  
	    if !@running then
	      logger.info "consumer #{thread_id}:shutting down, interrupted: #{Time.new.to_s}"
	      client.interrupt
	    end

	    now = Time.new
	   
	    if @running then
	      logger.debug "consumer #{thread_id}:next because < @min_batch_seconds" if (now - lastTime) < @min_batch_seconds
	      next if (now - lastTime) < @min_batch_seconds
	    end

	    lag = -1 # lag unknown
	    if block_given? then
	      # block is code to detect lag
	      lag = block.call(line, now)
	      logger.debug "consumer #{thread_id}:lag handler called, calculated lag: #{lag}"
	    end

	    @stats.add_consumed(intervalCount, intervalSize, lag)

	    intervalElapsedTime = now - lastTime

	    message_set_params = { num_records: intervalCount, size_bytes: intervalSize, time_sec: intervalElapsedTime.round(2).to_s, records_per_sec: (intervalCount / intervalElapsedTime).round(2).to_s, kbytes_per_sec: (intervalSize / intervalElapsedTime / 1024).round(2).to_s, message_lag: lag }

	    production_job = ProductionJob.new(@run_id, messages, message_set_params)
	    @production_queue << production_job
	    logger.info "consumer #{thread_id}:production job queued, #{messages.size} messages for #{@run_id}, job id #{production_job.id}, queue length #{@production_queue.length}"

	    production_job = nil
	    messages = []
	    lastTime = Time.new
	    intervalSize = 0
	    intervalCount = 0
	  end
	}

	logger.info "consumer #{thread_id}:shut down complete: #{Time.new.to_s}"
	logger.info "consumer #{thread_id}:total elapsed time: #{(Time.new - startTime).round(2).to_s} seconds"

      rescue Exception => e
	logger.error "Consumer thread #{thread_id}:Exception:#{e}"
      end
    end

  end

end
