module IOHelper
  class ChefLogger
    def info(message)
      Chef::Log.info(message)
    end

    def debug(message)
      Chef::Log.debug(message)
    end

    def warn(message)
      Chef::Log.warn(message)
    end

    def fatal(message)
      Chef::Log.fatal(message)
    end
  end

  @logger = ChefLogger.new

  def self.logger
    @logger
  end
end

module Settings
  require 'pp' if Chef::Config[:log_level] == :debug

  @script_dir = File.expand_path(File.dirname(__FILE__))

  def self.environment
    {
      'datacenter' => '',
      'policy_group' => 'local',
      'datacenters' => %w(),
    }
  end

  def self.paths
    {
      'cache_path' => "#{Chef::Config[:file_cache_path]}/sql_helper",
      'script_dir' => "#{@script_dir.tr('/', '\\')}\\..\\files",
      'powershell_helper_script' => "#{@script_dir}/../files/sql_helper.ps1",
      'sql_script_dir' => "#{File.dirname(__FILE__)}/sql",
      'replication_scripts' => "#{Chef::Config[:file_cache_path]}/sql_helper/replication",
      'logins_export_folder' => "#{Chef::Config[:file_cache_path]}/sql_helper/logins",
    }
  end

  def self.backups
    {
      'always_on_backup_temp_dir' => '',
      'free_space_threshold' => 15, # minimum percent free space after backup required for primary destination
      'default_destination' => '', # This should be a UNC path. In most cases this is a network share with ample space
      'default_backup_share' => 'SqlBackup', # The default share to check first when looking for backup files on a server
      'compress_backups' => false,
    }
  end

  def self.exports
    {
      'include_table_permissions' => false,
    }
  end
end
