require 'bundler/setup'
require 'logger'

require 'coveralls'
Coveralls.wear! if ENV["COVERALLS"]

require 'simplecov'
SimpleCov.start do
  add_filter "/spec/"
end

require 'stream_consumer'

RSpec.configure do |config|

  StreamConsumer.logger.console = true
  StreamConsumer.logger.level = Logger::INFO
  StreamConsumer.logger.logfile = true
  StreamConsumer.logger.tag = "rspec"

  config.filter_run_excluding disabled: true

end

def logger
  StreamConsumer.logger
end
