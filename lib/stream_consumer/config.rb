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

module StreamConsumer

  class Config < Hash

    @@singleton = nil

    def self.instance(config_file_path)
      return @@singleton unless @@singleton.nil?
      @@singleton = StreamConsumer::Config.new(config_file_path)
      logger.debug "config: #{@@singleton}"
      @@singleton
    end

    def stream_url
      return self[:stream_url] if self[:stream_params].nil?
      return self[:stream_url]+"?"+to_params(stream_params)
    end

    def stream_params
      unsymbolize_keys(self[:stream_params])
    end

    protected

    def initialize(config_file_path)
      @config = YAML.load_file(File.new(config_file_path, "r"))
      self.merge!(@config)
      symbolize_keys(self)
    end

    def symbolize_keys(h)
      h.keys.each do |key|
	if h[key].instance_of? Hash then
	  h[(key.to_sym rescue key) || key] = symbolize_keys(h.delete(key))
	elsif h[key].instance_of? Array then
	  h[(key.to_sym rescue key) || key] = h.delete(key).map do |x|
	    if x.instance_of? Hash then symbolize_keys(x) else x end
	  end
	else
	  h[(key.to_sym rescue key) || key] = h.delete(key)
	end
      end
      h
    end

    def unsymbolize_keys(h)
      h.keys.each do |key|
	if h[key].instance_of? Hash then
	  h[(key.to_s rescue key) || key] = unsymbolize_keys(h.delete(key))
	elsif h[key].instance_of? Array then
	  h[(key.to_s rescue key) || key] = h.delete(key).map do |x|
	    if x.instance_of? Hash then unsymbolize_keys(x) else x end
	  end
	else
	  h[(key.to_s rescue key) || key] = h.delete(key)
	end
      end
      h
    end

    def to_params(h)
      params = ''
      stack = []

      h.each do |k, v|
	if v.is_a?(Hash)
	  stack << [k,v]
	elsif v.is_a?(Array)
	  stack << [k,Hash.from_array(v)]
	else
	  params << "#{k}=#{v}&"
	end
      end

      stack.each do |parent, hash|
	hash.each do |k, v|
	  if v.is_a?(Hash)
	    stack << ["#{parent}[#{k}]", v]
	  else
	    params << "#{parent}[#{k}]=#{v}&"
	  end
	end
      end

      params.chop! 
      params
    end

    def from_array(array = [])
      h = Hash.new
      array.size.times do |t|
	h[t] = array[t]
      end
      h
    end

  end
end
