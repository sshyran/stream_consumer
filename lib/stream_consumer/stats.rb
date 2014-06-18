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

module StreamConsumer

  class Stats

    include Loggable

    class InterruptRequest < StandardError; end

    DEFAULT_STATS_CHECKPOINT_INTERVAL = 60

    attr_accessor :last_time
    attr_accessor :lag
    attr_accessor :messages_consumed
    attr_accessor :messages_produced
    attr_accessor :bytes_consumed

    def initialize(tag = nil)
      @tag = tag
      @mutex = Mutex.new
      clear(Time.new)
    end

    def clear(last_time)
      @last_time = last_time
      @lag = -1
      @messages_consumed = 0
      @messages_produced = 0
      @bytes_consumed = 0
    end

    def add_consumed(messages, bytes, lag)
      @mutex.synchronize {
	@messages_consumed = @messages_consumed + messages
	@bytes_consumed = @bytes_consumed + bytes
	@lag = lag unless lag == -1
      }
    end

    def add_produced(messages)
      @mutex.synchronize {
	@messages_produced = @messages_produced + messages
      }
    end

    def checkpoint
      params = nil
      @mutex.synchronize {
	params = tally
	clear(Time.new)
      }
      return params
    end

    def start(checkpoint_interval = DEFAULT_STATS_CHECKPOINT_INTERVAL, &checkpoint_callback)
      @checkpoint_interval = checkpoint_interval
      @checkpoint_callback = checkpoint_callback
      @thread = Thread.new { process_stats }
    end

    def backtrace
      @thread.backtrace
    end

    def halt
      @thread.raise InterruptRequest.new "Interrupt Request"
      @thread.join
    end

    def process_stats
      begin
	while true
	  sleep @checkpoint_interval
	  cp = checkpoint
	  logger.info "Checkpoint: #{cp}"
	  @checkpoint_callback.call(cp) unless @checkpoint_callback.nil?
	end
      rescue InterruptRequest
	logger.info "stats:interrupt requested"
	cp = tally
	logger.info "Final checkpoint: #{cp}"
	@checkpoint_callback.call(cp) unless @checkpoint_callback.nil?
	logger.info "stats:shut down complete: #{Time.new.to_s}"
      rescue Exception => e
	logger.error "Process stats thread:Exception:#{e}"
      end
    end

    protected

    def tally
      params = Hash.new
      now = Time.new
      elapsed_time = now - @last_time
      params[:tag] = @tag unless @tag.nil?
      params[:messages_consumed_per_sec] = (@messages_consumed / elapsed_time).round(2)
      params[:messages_produced_per_sec] = (@messages_produced / elapsed_time).round(2)
      params[:kbytes_consumed_per_sec] = (@bytes_consumed / elapsed_time / 1024).round(2)
      params[:lag] = @lag.round(3)
      params[:timestamp] = now
      params
    end

  end

end
