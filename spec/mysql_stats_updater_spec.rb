require 'spec_helper'

require 'stream_consumer/updater/mysql_stats_updater'

describe StreamConsumer::Updater::MysqlStatsUpdater do

  describe "test mysql stats updater" do

    it "should successfully connect to mysql and udate stats" do

      expect {
	updater = StreamConsumer::Updater::MysqlStatsUpdater.new(config[:kafka][:topic_name], config[:database])
	updater.update(CHECKPOINT_DATA.merge({timestamp: Time.new}))
      }.to_not raise_error

    end

  end

end
