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

  class ProductionJob

    attr_reader :topic_name
    attr_reader :messages
    attr_reader :params
    attr_reader :id

    @@production_job_id = 1

    def initialize(topic_name, messages, params)
      @topic_name = topic_name
      @messages = messages
      @params = params
      @id = @@production_job_id
      @@production_job_id = @@production_job_id+1
    end

    def clear
      @messages.clear
      @params.clear
      @messages = nil
      @topic_name = nil
      @params = nil
    end

  end

end
