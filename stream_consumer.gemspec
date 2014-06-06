# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'stream_consumer/version'

Gem::Specification.new do |spec|
  spec.name          = "stream_consumer"
  spec.version       = StreamConsumer::VERSION
  spec.authors       = ["David Tompkins"]
  spec.email         = ["tompkins@adobe.com"]
  spec.summary       = %q{Multi-threaded HTTP}
  spec.description   = %q{Multi-threaded HTTP stream consumer, with asynchronous spooling and multi-threaded production}
  spec.homepage      = "https://github.com/adobe-research/stream_consumer"
  spec.license       = "Apache 2.0"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake", '~> 10.1', '>= 10.1.1'
  spec.add_development_dependency "rspec", '~> 2.13', '>= 2.13.0'
  spec.add_development_dependency "simplecov", '~> 0.7', '>= 0.7.1'
  spec.add_development_dependency "coveralls", '~> 0.7', '>= 0.7.0'
  spec.add_runtime_dependency "http_streaming_client", '~> 0.9.5'
  spec.add_runtime_dependency "poseidon"
  spec.add_runtime_dependency "jdbc-mysql" if defined?(JRUBY_VERSION)
  spec.add_runtime_dependency "mysql2" if !defined?(JRUBY_VERSION)
end
