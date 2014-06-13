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

require 'logger'

module StreamConsumer

  class BasicLogFormatter < Logger::Formatter

    def call(severity, time, progname, msg)
      "[%s] %s%s - %s\n" % [time.to_s, severity, @tag.nil? ? "" : "(#{@tag})", msg]
    end

    def tag=(tag)
      @tag = tag
    end

  end

  class CustomLoggerInternal

    def initialize
      @console = nil
      @logfile = nil
    end

    def method_missing(name, *args)
      if !@console.nil?
	@console.method(name).call(args) unless name.to_s =~ /(unknown|fatal|error|warn|info|debug)/
	@console.method(name).call(args[0])
      end
      @logfile.method(name).call(args[0]) unless @logfile.nil?
    end

    def logfile=(filepath)
      return (@logfile = nil) if filepath.nil?
      @logfile = Logger.new(filepath)
      @logfile.formatter = BasicLogFormatter.new
      @logfile.level = Logger::DEBUG
    end

    def console=(enable)
      return (@console = nil) if !enable
      @console = Logger.new(STDOUT)
      @console.formatter = BasicLogFormatter.new
      @console.level = Logger::INFO
    end

    def tag=(tag)
      @console.formatter.tag = tag unless @console.nil?
      @logfile.formatter.tag = tag unless @logfile.nil?
    end

  end

  module Loggable
    def self.logger
      StreamConsumer.logger
    end

    def logger
      StreamConsumer.logger
    end
  end

  @custom_logger_internal = nil

  def self.logger
    return @custom_logger_internal unless @custom_logger_internal.nil?
    return @custom_logger_internal = CustomLoggerInternal.new
  end

  def self.logger=(logger)
    @custom_logger_internal = logger
  end

end
