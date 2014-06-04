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
  module Updater

    class ConsoleStatsUpdater < StatsUpdater

      def initialize
	@inbound_id = "inbound_id"
      end

      def update(checkpoint)
	query = "insert into `inbound_stats_v2` (`created_at`, `inbound_id`, `consumed_records_per_sec`, `consumed_kbytes_per_sec`, `produced_records_per_sec`, `message_lag`) values ('#{params[:timestamp].utc.strftime('%Y-%m-%d %H:%M:%S')}', #{@inbound_id}, #{params[:messages_consumed_per_sec]}, #{params[:kbytes_consumed_per_sec]}, #{params[:messages_produced_per_sec]}, #{params[:lag]})"
	logger.info "#{query}"
      end

    end

  end
end
