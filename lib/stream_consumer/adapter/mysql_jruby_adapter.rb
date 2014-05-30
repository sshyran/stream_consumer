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

require 'jdbc/mysql'

Jdbc::MySQL.load_driver
java_import 'java.sql.DriverManager' 

module StreamConsumer
  module Adapter
    
    class MySqlAdapter

      def initialize(config, logger)

	if !defined?(JRUBY_VERSION) then
	  raise new SystemError "JRUBY_VERSION not defined in MySqlAdapter(JRuby)"
	end

	@config = config
	@logger = logger

	@logger.info "MySqlAdapter(JRuby):database config: #{@config}"

	@host = @config['host'] || "localhost"
	@port = @config['port'] || "3306"
	@jdbc_url = "jdbc:mysql://#{@host}:#{@port}/#{@config['database']}"
	@username = @config['username']
	@password = @config['password']
	@connection = nil
      end

      def query(query)
	@logger.debug "MySqlAdapter(JRuby):query:#{query}"
	statement = client.createStatement
	result_set = statement.executeQuery(query)
	@logger.debug "MySqlAdapter(JRuby):result_set:#{result_set}"
	result_set_hash = resultset_to_hash(result_set)
	@logger.debug "MySqlAdapter(JRuby):result_set_hash:#{result_set_hash}"
	result_set_hash
      end

      def update(update)
	@logger.debug "MySqlAdapter(JRuby):update:#{update}"
	statement = client.createStatement
	count = statement.executeUpdate(update)
	@logger.debug "MySqlAdapter(JRuby):rows updated:#{count}"
	count
      end

      protected

      def client
	if @connection.nil? then
	  @logger.debug "MySqlAdapter(JRuby):creating connection..."
	  @connection = DriverManager.getConnection(@jdbc_url, @username, @password)
	  @logger.debug "MySqlAdapter(JRuby):connection:#{@connection}"
	end
	@connection
      end

      def resultset_to_hash(resultset)
	meta = resultset.meta_data
	rows = []

	while resultset.next
	  row = {}

	  (1..meta.column_count).each do |i|
	    name = meta.column_name i
	    row[(name.to_sym rescue name) || name] = case meta.column_type(i)
						     when -6, -5, 5, 4
						       # TINYINT, BIGINT, INTEGER
						       resultset.get_int(i).to_i
						     when 41
						       # Date
						       resultset.get_date(i)
						     when 92
						       # Time
						       resultset.get_time(i).to_i
						     when 93
						       # Timestamp
						       resultset.get_timestamp(i)
						     when 2, 3, 6
						       # NUMERIC, DECIMAL, FLOAT
						       case meta.scale(i)
						       when 0
							 resultset.get_long(i).to_i
						       else
							 BigDecimal.new(resultset.get_string(i).to_s)
						       end
						     when 1, -15, -9, 12
						       # CHAR, NCHAR, NVARCHAR, VARCHAR
						       resultset.get_string(i).to_s
						     else
						       resultset.get_string(i).to_s
						     end
	  end
	  rows << row
	end
	rows
      end

    end
  end
end
