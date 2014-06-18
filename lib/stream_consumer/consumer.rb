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

    class InterruptRequest < StandardError; end

    def initialize(config)
      HttpStreamingClient.logger = logger
      @config = config
      @production_queue = Queue.new
    end

    def halt
      @consumer_threads.each { |thread| thread[:client].interrupt }
      @consumer_threads.each { |thread| thread.raise InterruptRequest.new "Interrupt Request" }
      @consumer_threads.each { |thread| thread.join }

      @producer_threads.each { |thread| thread.raise InterruptRequest.new "Interrupt Request"}
      @producer_threads.each { |thread| thread.join }

      @stats.halt
      
      @debug.raise InterruptRequest.new "Interrupt Request" if defined? @debug
    end

    def produce_messages(thread_id)
      begin
	startTime = Time.new 
	logger.info "starting producer #{thread_id} for id #{@run_id}: #{startTime.to_s}"
	while true
	  job = @production_queue.pop
	  logger.info "Production job accepted: thread #{thread_id}, job id #{job.id}, run id #{job.run_id}"
	  now = Time.new

	  @config[:data_producer].produce(thread_id, job.messages) unless @config[:data_producer].nil?
	  @stats.add_produced(job.messages.size)
	  msg = "#{job.messages.size} messages produced to run id #{job.run_id}, job id #{job.id}"

	  job.clear
	  job = nil
	  elapsed_time = Time.new - now
	  logger.info "#{msg}, production elapsed time: #{elapsed_time} seconds"
	end
	logger.info "producer #{thread_id}:shut down complete: #{Time.new.to_s}"
	logger.info "producer #{thread_id}:total elapsed time: #{(Time.new - startTime).round(2).to_s} seconds"
      rescue InterruptRequest
	logger.info "producer #{thread_id}:interrupt requested"
	logger.info "producer #{thread_id}:shut down complete: #{Time.new.to_s}"
	logger.info "producer #{thread_id}:total elapsed time: #{(Time.new - startTime).round(2).to_s} seconds"
      rescue Exception => e
	logger.error "Production job thread #{thread_id}:Exception:#{e}"
      end
    end

    def debug_threads
      begin
	sleep 30
	logger.info "-----------------------------------"
	@consumer_threads.each do |thread|
	  logger.info "----consumer #{thread[:thread_id]}:"
	  thread.backtrace.each { |line| logger.info line }
	end
	@producer_threads.each do |thread|
	  logger.info "----producer #{thread[:thread_id]}:"
	  thread.backtrace.each { |line| logger.info line }
	end
	logger.info "----stats thread"
	@stats.backtrace.each { |line| logger.info line }
	logger.info "-----------------------------------"
      rescue InterruptRequest
	logger.info "debug_threads:interrupt requested"
	logger.info "debug_threads:shut down complete: #{Time.new.to_s}"
      end
    end

    def run(&block)

      @run_id = @config[:kafka][:client_id]
      @records_per_batch = @config[:records_per_batch]
      @min_batch_seconds = @config[:min_batch_seconds]

      logger.info "-----------------------------------------------------------------"
      logger.info "url: #{@config.stream_url}"
      logger.info "run_id: #{@run_id}"
      logger.info "consumer threads: #{@config[:num_consumer_threads]}"
      logger.info "producer threads: #{@config[:num_producer_threads]}"
      logger.info "-----------------------------------------------------------------"

      @stats = Stats.new(@config[:kafka][:client_id])
      @stats.start { |checkpoint| @config[:stats_updater].update(checkpoint) unless @config[:stats_updater].nil? }

      @producer_threads = (1..@config[:num_producer_threads]).map { |i| Thread.new(i) { |thread_id| Thread.current[:thread_id] = thread_id; produce_messages(thread_id) } }
      @consumer_threads = (1..@config[:num_consumer_threads]).map { |i| Thread.new(i) { |thread_id| Thread.current[:thread_id] = thread_id; consume_messages(thread_id, &block) } }

      @debug = Thread.new { debug_threads } if @config[:debug_threads]

      @consumer_threads.each { |thread| thread.join }
      @producer_threads.each { |thread| thread.join }
      @stats.halt
      @debug.kill

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

	if @config[:reconnect] then
	  client = HttpStreamingClient::Client.new(compression: true, reconnect: true, reconnect_interval: @config[:reconnect_interval], reconnect_attempts: @config[:reconnect_attempts])
	else
	  client = HttpStreamingClient::Client.new(compression: true)
	end

	Thread.current[:client] = client

	startTime = lastTime = Time.new 

	logger.info "client.get:#{@config.stream_url}, #{@config[:options_factory].get_options}"
	response = client.get(@config.stream_url, { :options_factory => @config[:options_factory] }) { |line|

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

	  if !@config[:signal_prefix_array].nil? then
	    signal_message_received = false
	    @config[:signal_prefix_array].each do |prefix|
	      if line.start_with? prefix then
		logger.info "consumer #{thread_id}:Stream signal message received for id:#{@run_id}:#{line}"
		signal_message_received = true
		break
	      end
	    end
	    next if signal_message_received
	  end

	  logger.debug "consumer #{thread_id}:line: #{line}"

	  messages << @config[:data_producer].format(line) unless @config[:data_producer].nil?

	  totalCount = totalCount + 1
	  intervalCount = intervalCount + 1
	  intervalSize = intervalSize + line.size

	  if (totalCount % @records_per_batch) == 0 then

	    now = Time.new

	    if (now - lastTime) < @min_batch_seconds then
	      logger.debug "consumer #{thread_id}:next because < @min_batch_seconds"
	      next
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
	    logger.info "consumer #{thread_id}:enqueuing production job, #{messages.size} messages for #{@run_id}, job id #{production_job.id}, queue length #{@production_queue.length}"
	    @production_queue << production_job

	    production_job = nil
	    messages = []
	    lastTime = Time.new
	    intervalSize = 0
	    intervalCount = 0
	  end
	}

	logger.info "consumer #{thread_id}:shut down complete: #{Time.new.to_s}"
	logger.info "consumer #{thread_id}:total elapsed time: #{(Time.new - startTime).round(2).to_s} seconds"

      rescue InterruptRequest
	logger.info "consumer #{thread_id}:interrupt requested"
	#client.interrupt
	logger.info "consumer #{thread_id}:shut down complete: #{Time.new.to_s}"
      rescue Exception => e
	logger.error "Consumer thread #{thread_id}:Exception:#{e}"
	logger.error "Backtrace:\n\t#{e.backtrace.join("\n\t")}"
      end
    end

  end

end
