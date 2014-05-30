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

require 'mysql2'

module StreamConsumer
  module Adapter

    class MySqlAdapter

      def initialize(config, logger)

	if defined?(JRUBY_VERSION) then
	  raise new SystemError "JRUBY_VERSION is defined in MySqlAdapter(MRI)"
	end

	@config = config
	@logger = logger

	@logger.info "MySqlAdapter(MRI):database config: #{@config}"
	@connection = nil
      end

      def query(query)
	@logger.debug "MySqlAdapter(MRI):query:#{query}"
	result_set = client.query(query, :as => :hash)
	@logger.debug "MySqlAdapter(MRI):result_set:#{result_set_to_s(result_set)}"
	result_set.each { |result| symbolize_keys(result) } unless result_set.nil?
	@logger.debug "MySqlAdapter(MRI):result_set_hash:#{result_set_to_s(result_set)}"
	result_set
      end

      def update(update)
	query(update)
      end

      protected

      def client
	if @connection.nil? then
	  @logger.debug "MySqlAdapter(MRI):creating connection..."
	  @connection = Mysql2::Client.new(@config)
	  @logger.debug "MySqlAdapter(MRI):connection:#{@connection}"
	end
	@connection
      end

      def result_set_to_s(rs)
	return "" if rs.nil? or rs.count == 0
	s = "["
	rs.each {|row| s = s + row.to_s + ","}
	s = s+"]"
      end

      def symbolize_keys(h)
	h.keys.each do |key|
	  h[(key.to_sym rescue key) || key] = h.delete(key)
	end
	h
      end

    end

  end
end
