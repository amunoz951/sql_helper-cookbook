resource_name :sql_helper_database

property :database_name, String, name_property: true
property :sql_server, String, required: true
property :connection_string, String, required: true
property :backup_start_time, [String, Time], required: true # The minimum timestamp a backup should have to be considered current
property :free_space_threshold, Integer, default: 15 # minimum percent free space after backup required for primary destination
property :default_destination, String, required: true # This should be a UNC path. In most cases this is a network share with ample space
deprecated_property_alias(:alternate_destination, :default_destination, 'The sql_helper_database property `alternate_destination` has been renamed to `default_destination`! Please update your cookbooks to use the new property name.')
property :synchronous_timeout, Integer, default: 0 # How long to wait to see if backup completes in seconds

default_action :backup

action :backup do
  new_resource.sensitive = true if connection_string.downcase.include?('password')
  new_resource.backup_start_time = Time.parse(backup_start_time) if backup_start_time.is_a?(String)
  extend WindowsConfiguration
  extend SqlHelper

  sql_server_settings = get_backup_sql_server_settings(connection_string)
  backup_basename = "#{database_name}_#{Time.now.strftime('%Y%m%d')}"

  primary_backup_files = sql_server_backup_files(sql_server_settings, backup_basename)
  backup_files = primary_backup_files.empty? ? get_unc_backup_files(default_destination, backup_basename) : primary_backup_files

  unless backup_files.empty?
    sql_backup_header = get_sql_backup_headers(connection_string, backup_files).first
    unless sql_backup_header.nil?
      Chef::Log.info "Last backup for #{database_name} completed: #{sql_backup_header['BackupFinishDate']}"
      backup_finish_time = Time.strptime(sql_backup_header['BackupFinishDate'], '%m/%d/%Y %H:%M:%S')
      return if backup_finish_time > backup_start_time
    end
  end

  database_size = get_database_size(connection_string, database_name)
  sql_server_disk_space = get_sql_disk_space(sql_server_settings['connection_string'], sql_server_settings['BackupDir'])
  sql_server_free_space = sql_server_disk_space['Available_MB'].to_f
  sql_server_disk_size = sql_server_disk_space['Total_MB'].to_f
  alternate_share_free_space = get_disk_free_space(default_destination) - database_size
  sql_server_free_space_percentage = sql_server_disk_size.nil? ? 'unknown ' : (sql_server_free_space / sql_server_disk_size) * 100
  Chef::Log.info "Free space on SQL server backup drive: #{sql_server_free_space_percentage.round(2)}%"

  backup_folder = if sql_server_free_space_percentage >= free_space_threshold
                    sql_server_settings['BackupDir']
                  elsif alternate_share_free_space > 0
                    default_destination
                  else
                    raise "Failed to backup database #{database_name} due to insufficient space. \n"\
                          "  Space after backup on #{default_destination}: #{alternate_share_free_space} \n\n"\
                          "  Space after backup on #{sql_server}: #{sql_server_free_space_percentage}% \n"\
                          '  Backup manually before retrying or specify to bypass backup in customers json.'
                  end

  run_sql_backup(connection_string, backup_folder, database_name, backup_basename, sql_server_settings['CompressBackup'])

  timeout_increment = 5
  backup_status_script = ::File.read("#{Chef::Config['file_cache_path']}/cookbooks/sql_helper/files/BackupProgress.sql")
  while synchronous_timeout > 0
    sleep(timeout_increment)
    break if execute_reader(connection_string, backup_status_script).empty? # Exit loop if no backups are in progress
    new_resource.synchronous_timeout = synchronous_timeout - timeout_increment
  end
end

action :check_backup_status do
  new_resource.sensitive = true if connection_string.downcase.include?('password')
  new_resource.backup_start_time = Time.parse(backup_start_time) if backup_start_time.is_a?(String)
  extend WindowsConfiguration
  extend SqlHelper

  sql_server_settings = get_backup_sql_server_settings(connection_string)
  backup_basename = "#{database_name}_#{Time.now.strftime('%Y%m%d')}"

  primary_backup_files = sql_server_backup_files(sql_server_settings, backup_basename)
  backup_files = primary_backup_files.empty? ? get_unc_backup_files(default_destination, backup_basename) : primary_backup_files

  if backup_files.empty?
    node.run_state["#{database_name}_backup"] = 'incomplete'
    return
  end

  sql_backup_header = get_sql_backup_headers(connection_string, backup_files).first
  if sql_backup_header.nil?
    node.run_state["#{database_name}_backup"] = 'incomplete'
    return
  end

  Chef::Log.info "Last backup for #{database_name} completed: #{sql_backup_header['BackupFinishDate']}"
  backup_finish_time = Time.strptime(sql_backup_header['BackupFinishDate'], '%m/%d/%Y %H:%M:%S')
  unless backup_finish_time > backup_start_time
    node.run_state["#{database_name}_backup"] = 'incomplete'
    return
  end

  node.run_state["#{database_name}_backup"] = 'current'
  Chef::Log.info 'Backup is current.'
end

def get_unc_backup_files(backup_folder, backup_basename)
  backup_files = if ::File.exist?("#{backup_folder}\\#{backup_basename}.bak")
                   ["#{backup_folder}\\#{backup_basename}.bak"]
                 else
                   Dir.glob("#{backup_folder}\\#{backup_basename}.part*.bak")
                 end
  backup_files = Dir.glob("#{backup_folder}\\#{backup_basename}*.bak") if backup_files.empty?
  backup_files
end
