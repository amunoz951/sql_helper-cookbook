require 'json'
require 'open3'
require 'fileutils'

module SqlHelper
  @verbose = true # show queries and returned json

  module_function

  def sql_script_dir
    Settings.paths['sql_script_dir']
  end

  # Execute a SQL query and return a dataset, table, row, or scalar, based on the return_type specified
  #   return_type: :all_tables, :first_table, :first_row, :scalar
  #   values: hash of values to replace sqlcmd style variables.
  #           EG: sql_query = SELECT * FROM $(databasename)
  #               values = { 'databasename' => 'my_database_name' }
  #   readonly: if true, sets the connection_string to readonly (Useful with AOAG)
  #   retries: number of times to re-attempt failed queries
  #   retry_delay: how many seconds to wait between retries
  def execute_query(connection_string, sql_query, return_type: :all_tables, values: nil, timeout: 172_800, readonly: false, ignore_missing_values: false, retries: 0, retry_delay: 5)
    sql_query = insert_values(sql_query, values) unless values.nil?
    missing_values = sql_query.reverse.scan(/(\)[0-9a-z_]+\(\$)(?!.*--)/i).uniq.join(' ,').reverse # Don't include commented variables
    raise "sql_query has missing variables! Ensure that values are supplied for: #{missing_values}\n" unless missing_values.empty? || ignore_missing_values
    connection_string_updated = connection_string.dup
    connection_string_updated = replace_connection_string_part(connection_string, :applicationintent, 'readonly') if readonly
    raise 'Connection string is nil or incomplete' if connection_string_updated.nil? || connection_string_updated.empty?
    IOHelper.logger.debug("Executing query with connection_string: \n\t#{hide_connection_string_password(connection_string_updated)}")
    IOHelper.logger.debug(sql_query) if @verbose && sql_query.length < 8096
    start_time = Time.now.strftime('%y%m%d_%H%M%S-%L')
    FileUtils.mkdir_p "#{Settings.paths['cache_path']}/scripts"
    FileUtils.cp(Settings.paths['powershell_helper_script'], "#{Settings.paths['cache_path']}\\scripts\\sql_helper_#{start_time}.ps1")
    ps_script = <<-EOS.strip
        . "#{Settings.paths['cache_path']}\\scripts\\sql_helper_#{start_time}.ps1"

        $connectionString = '#{connection_string_updated}'
        $sqlCommand = '#{sql_query.gsub('\'', '\'\'')}'
        $dataset = Invoke-SQL -timeout #{timeout} -connectionString $connectionString -sqlCommand $sqlCommand
        ConvertSqlDatasetTo-Json -dataset $dataset
      EOS

    ps_script_file = "#{Settings.paths['cache_path']}/scripts/ps_script-thread_id-#{Thread.current.object_id}.ps1"
    ::File.write(ps_script_file, ps_script)
    retry_count = 0
    begin
      result = ''
      exit_status = ''
      Open3.popen3("powershell -File \"#{ps_script_file}\"") do |_stdin, stdout, stderr, wait_thread|
        buffers = [stdout, stderr]
        queued_buffers = IO.select(buffers) || [[]]
        queued_buffers.first.each do |buffer|
          case buffer
          when stdout
            while (line = buffer.gets)
              stdout_split = line.split('json_tables:')
              raise "SQL exception: #{stdout_split.first} #{stdout.read} #{stderr.read}\nConnectionString: '#{hide_connection_string_password(connection_string)}'\n#{FormatHelper.terminal_line('=')}\n" if stdout_split.first =~ /error 50000, severity (1[1-9]|2[0-5])/i
              IOHelper.logger.info "SQL message: #{stdout_split.first.strip}" unless stdout_split.first.empty?
              result = stdout_split.last + buffer.read if stdout_split.count > 1
            end
          when stderr
            error_message = stderr.read
            raise "SQL exception: #{error_message}\nConnectionString: '#{hide_connection_string_password(connection_string)}'\n#{'=' * 120}\n" unless error_message.empty?
          end
        end
        exit_status = wait_thread.value
      end
      IOHelper.logger.debug "Script exit status: #{exit_status}"
      IOHelper.logger.debug "JSON result: #{result}"
    rescue
      retry_message = 'Executing SQL query failed! '
      retry_message += if retries == 0
                         'No retries specified. Will not reattempt. '
                       else
                         retry_count < retries ? "Retry #{(retry_count + 1)} of #{retries}" : "All #{retries} retries attempted."
                       end
      IOHelper.logger.info retry_message
      if (retry_count += 1) <= retries
        IOHelper.logger.info "Retrying in #{retry_delay} seconds..."
        sleep(retry_delay)
        retry
      end
      raise
    end

    begin
      convert_powershell_tables_to_hash(result, return_type)
    rescue # Change it to use terminal size instead of 120 chars in the error here and above
      IOHelper.logger.fatal "Failed to convert SQL data to hash! ConnectionString: '#{hide_connection_string_password(connection_string)}'\n#{FormatHelper.terminal_line('=')}\n"
      raise
    end
  ensure
    ::File.delete "#{Settings.paths['cache_path']}\\scripts\\sql_helper_#{start_time}.ps1" if defined?(start_time) && ::File.exist?("#{Settings.paths['cache_path']}\\scripts\\sql_helper_#{start_time}.ps1")
  end

  def convert_powershell_tables_to_hash(json_string, return_type = :all_tables) # options: :all_tables, :first_table, :first_row
    IOHelper.logger.debug "Output from sql command: #{json_string}" if @verbose
    result_hash = if json_string.empty?
                    {}
                  else
                    convert_javascript_dates_to_time_objects(JSON.parse(to_utf8(json_string.sub(/[^{\[]*/, ''))))
                  end
    raise 'No tables were returned by specified sql query!' if result_hash.values.first.nil? && return_type != :all_tables
    case return_type
    when :first_table
      result_hash.values.first
    when :first_row
      result_hash.values.first.first
    when :scalar
      return nil if result_hash.values.first.first.nil?
      result_hash.values.first.first.values.first # Return first column of first row of first table
    else
      result_hash
    end
  end

  def convert_javascript_dates_to_time_objects(value)
    case value
    when Array
      value.map { |v| convert_javascript_dates_to_time_objects(v) }
    when Hash
      Hash[value.map { |k, v| [k, convert_javascript_dates_to_time_objects(v)] }]
    else
      javascript_time = value[%r{(?<=/Date\()\d+(?=\)/)}, 0].to_f if value.is_a?(String)
      # TODO: handle varbinary data
      javascript_time.nil? || javascript_time == 0 ? value : Time.at(javascript_time / 1000.0)
    end
  end

  def connection_string_accessible?(connection_string, suppress_failure: true)
    sql_script = 'SELECT @@SERVERNAME AS [ServerName]'
    !execute_query(connection_string, sql_script, return_type: :scalar).nil?
  rescue
    raise unless suppress_failure
    false
  end

  def get_sql_server_settings(connection_string)
    get_sql_settings_script = ::File.read("#{sql_script_dir}/Status/SQLSettings.sql")
    sql_server_settings = execute_query(SqlHelper.remove_connection_string_part(connection_string, :database), get_sql_settings_script, return_type: :first_row)
    IOHelper.logger.debug "sql_server_settings: \n#{JSON.pretty_generate(sql_server_settings)}"
    return nil if sql_server_settings.nil? || sql_server_settings['ServerName'].nil?

    direct_connection_string = connection_string.gsub(sql_server_settings['DataSource'], sql_server_settings['ServerName'])
    application_connection_string = connection_string.gsub(sql_server_settings['ServerName'], sql_server_settings['DataSource'])
    secondary_replica_connection_string = if sql_server_settings['SecondaryReplica'].nil?
                                            nil
                                          else
                                            cnstr = SqlHelper.replace_connection_string_part(connection_string, :server, sql_server_settings['SecondaryReplica'])
                                            SqlHelper.remove_connection_string_part(cnstr, :database)
                                          end
    sql_server_settings['direct_connection_string'] = direct_connection_string # Does not use AlwaysOn listener
    sql_server_settings['connection_string'] = application_connection_string
    sql_server_settings['secondary_replica_connection_string'] = secondary_replica_connection_string
    sql_server_settings['DataDir'] = FormatHelper::Directory.ensure_trailing_slash(sql_server_settings['DataDir'])
    sql_server_settings['LogDir'] = FormatHelper::Directory.ensure_trailing_slash(sql_server_settings['LogDir'])
    sql_server_settings['BackupDir'] = FormatHelper::Directory.ensure_trailing_slash(sql_server_settings['BackupDir'])
    sql_server_settings
  end

  def get_backup_sql_server_settings(connection_string)
    sql_server_settings = get_sql_server_settings(connection_string)
    sql_server_settings = get_sql_server_settings(to_integrated_security(connection_string)) if sql_server_settings.nil? || sql_server_settings['BackupDir'].nil? || sql_server_settings['BackupDir'] == 'null'
    raise "FATAL: Current user #{ENV['user'] || ENV['username']} does not have access to backup database!" if sql_server_settings.nil? || sql_server_settings['BackupDir'].nil? || sql_server_settings['BackupDir'] == 'null'
    sql_server_settings
  end

  # Substitute sqlcmd style variables with values provided in a hash. Don't include $()
  # Example values: 'varname' => 'Some value', 'varname2' => 'some other value'
  def insert_values(sql_query, values, case_sensitive: false)
    return sql_query if values.nil? || values.all? { |i, _j| i.nil? || i.empty? }

    IOHelper.logger.debug "Inserting variable values into query: #{JSON.pretty_generate(values)}"
    sql_query = to_utf8(sql_query)
    values.each do |key, value|
      regexp_key = case_sensitive ? "$(#{key})" : /\$\(#{Regexp.escape(key)}\)/i
      sql_query.gsub!(regexp_key) { value }
    end

    sql_query
  end

  # Returns a single string to be used for the source for a RESTORE command from an array of backup file paths
  def backup_fileset_names(backup_files)
    result = ''
    backup_files.each { |backup_file| result << " DISK = N''#{backup_file}'',".tr('/', '\\') }
    result.chomp(',')
  end

  # Returns the database size in MB
  def get_database_size(connection_string, database_name)
    sql_script = ::File.read("#{sql_script_dir}/Status/DatabaseSize.sql")
    execute_query(connection_string, sql_script, values: { 'databasename' => database_name }, return_type: :scalar, readonly: true).to_f
  end

  # Returns a hash with the following fields: Available_MB, Total_MB, Percent_Free
  def get_sql_disk_space(connection_string, target_folder)
    sql_script = ::File.read("#{sql_script_dir}/Status/DiskSpace.sql")
    execute_query(connection_string, sql_script, return_type: :first_row, values: { 'targetfolder' => target_folder })
  end

  # Returns the headers from the backup set provided. Pass an array of path strings to the backup files.
  def get_sql_backup_headers(connection_string, backup_files)
    FormatHelper.validate_parameters(method(__method__), binding)
    disk_backup_files = backup_fileset_names(backup_files)
    sql_script = ::File.read("#{sql_script_dir}/Database/GetBackupHeaders.sql")
    execute_query(connection_string, sql_script, return_type: :first_table, values: { 'bkupfiles' => disk_backup_files })
  end

  def gap_in_log_backups?(sql_server_settings, database_backup_header, restored_database_lsn, backup_folder, backup_basename)
    last_full_backup_lsn = database_backup_header['LastLSN']
    return true if last_full_backup_lsn.nil? # if the last full backup does not exist, behave as if there is a gap in the log backups
    backup_sets = backup_sets_from_unc_path(sql_server_settings, backup_folder, backup_basename, database_backup_header: database_backup_header, restored_database_lsn: restored_database_lsn, log_only: true)
    return true if backup_sets.nil? # nil is returned if the backup is too new for the restored database LSN, therefore there's a gap
    return false if backup_sets.empty? # if no log backup sets were current, behave as if there is no gap since a log backup hasn't yet been taken since the backup
    first_lsn_from_log_backups = get_sql_backup_headers(sql_server_settings['connection_string'], backup_sets.first.last).first['FirstLSN']
    IOHelper.logger.debug "LastLSN from full backup: #{last_full_backup_lsn} | First LSN from log backups: #{first_lsn_from_log_backups}"
    last_full_backup_lsn < first_lsn_from_log_backups && restored_database_lsn < first_lsn_from_log_backups
  end

  # Returns the data and log file information contained in the backup files.
  def get_backup_file_info(connection_string, backup_files)
    FormatHelper.validate_parameters(method(__method__), binding)
    disk_backup_files = backup_fileset_names(backup_files)
    sql_script = ::File.read("#{sql_script_dir}/Database/GetFileInfoFromBackup.sql")
    execute_query(connection_string, sql_script, return_type: :first_table, values: { 'bkupfiles' => disk_backup_files })
  end

  def get_backup_logical_names(connection_string, backup_files)
    FormatHelper.validate_parameters(method(__method__), binding)
    sql_backup_file_info = SqlHelper.get_backup_file_info(connection_string, backup_files)
    data_file_logical_name = sql_backup_file_info.select { |file| file['Type'] == 'D' }.first['LogicalName']
    log_file_logical_name = sql_backup_file_info.select { |file| file['Type'] == 'L' }.first['LogicalName']
    [data_file_logical_name, log_file_logical_name]
  end

  def sql_server_backup_files(sql_server_settings, backup_basename, log_only: false)
    values = { 'targetfolder' => sql_server_settings['BackupDir'],
               'bkupname' => backup_basename,
               'logonly' => log_only }
    sql_script = ::File.read("#{sql_script_dir}/Database/GetBackupFiles.sql")
    backup_files_results = execute_query(sql_server_settings['connection_string'], sql_script, return_type: :first_table, values: values)
    backup_files = []
    backup_files_results.each do |file|
      backup_files.push("#{FormatHelper::Directory.ensure_trailing_slash(sql_server_settings['BackupDir'])}#{file['FileName']}")
    end
    if log_only
      database_backup_files = sql_server_backup_files(sql_server_settings, backup_basename)
      database_backup_header = get_sql_backup_headers(sql_server_settings['connection_string'], database_backup_files).first
      return relevant_log_backup_sets(sql_server_settings, backup_files, database_backup_header, 0)
    end
    most_recent_backup_files_and_basename(sql_server_settings, backup_files, backup_basename)
  end

  def most_recent_backup_files_and_basename(sql_server_settings, backup_files, backup_basename)
    backup_sets = backup_sets_from_backup_files(backup_files)
    if backup_sets.keys.count > 1 # if there is more than 1 backup set, find the most recent
      backup_headers = {}
      backup_sets.each do |basename, files|
        backup_headers[basename] = get_sql_backup_headers(sql_server_settings['connection_string'], files).first
      end
      backup_basename = backup_headers.max_by { |_basename, header| header['BackupFinishDate'] }.first
    elsif backup_sets.keys.count == 0 # if there are no backup sets, use an empty array
      backup_sets[backup_basename] = []
    end
    [backup_sets[backup_basename], backup_basename]
  end

  def relevant_log_backup_sets(sql_server_settings, backup_files, database_backup_header, restored_database_lsn)
    restored_database_lsn ||= 0
    backup_sets = backup_sets_from_backup_files(backup_files)
    database_backup_lsn = database_backup_header['DatabaseBackupLSN']
    IOHelper.logger.debug "Database backup LSN: #{database_backup_lsn}"
    backup_headers = backup_sets.each_with_object({}) { |(basename, files), headers| headers[basename] = get_sql_backup_headers(sql_server_settings['connection_string'], files).first }
    backup_sets = backup_sets.sort_by { |basename, _files| backup_headers[basename]['LastLSN'] }.to_h
    backup_headers = backup_headers.sort_by { |_basename, backup_header| backup_header['LastLSN'] }.to_h
    IOHelper.logger.debug "Backup sets after sorting: #{JSON.pretty_generate(backup_sets)}"
    backup_sets.each { |basename, _files| IOHelper.logger.debug "Backup header for #{basename}: FirstLSN: #{backup_headers[basename]['FirstLSN']} | LastLSN: #{backup_headers[basename]['LastLSN']}" }
    start_lsn = nil
    current_lsn = database_backup_header['LastLSN']
    backup_headers.each do |basename, backup_header|
      start_lsn ||= backup_header['FirstLSN']
      IOHelper.logger.debug "Current LSN: #{current_lsn}"
      IOHelper.logger.debug "Current header (#{basename}) - FirstLSN: #{backup_headers[basename]['FirstLSN']} | LastLSN: #{backup_headers[basename]['LastLSN']} | DatabaseBackupLSN: #{backup_headers[basename]['DatabaseBackupLSN']}"
      unless backup_header['DatabaseBackupLSN'] == database_backup_lsn
        IOHelper.logger.debug "Current backup is from a different database backup as the DatabaseBackupLSN (#{backup_header['DatabaseBackupLSN']}) doesn't match the database backup LSN (#{database_backup_lsn}). Removing backup set..."
        backup_sets.delete(basename)
        next
      end
      if backup_header['LastLSN'] < database_backup_lsn || backup_header['LastLSN'] < restored_database_lsn
        IOHelper.logger.debug "Current backup LastLSN (#{backup_header['FirstLSN']}) older than database backup LSN (#{database_backup_lsn}) or restored database LSN (#{restored_database_lsn}). Removing backup set..."
        backup_sets.delete(basename)
        next
      end
      if backup_header['FirstLSN'] > current_lsn # remove previous backup sets if there's a gap
        IOHelper.logger.debug "Gap found between previous backup LastLSN (#{current_lsn}) and current backup FirstLSN #{backup_header['FirstLSN']}. Updating starting point..." unless current_lsn == 0
        start_lsn = backup_header['FirstLSN']
        if start_lsn > restored_database_lsn # if the starting point is beyond the restored database LastLSN, no backups can be applied
          IOHelper.logger.warn "Gap found in log backups. The previous backup ends at LSN #{current_lsn} and the next log backup starts at LSN #{backup_header['FirstLSN']}!"
          return nil
        end
      end
      current_lsn = backup_header['LastLSN']
    end
    backup_headers.each { |basename, backup_header| backup_sets.delete(basename) unless backup_header['LastLSN'] > start_lsn } # remove any obsolete backup sets
    IOHelper.logger.debug "Backup sets after removing obsolete sets: #{JSON.pretty_generate(backup_sets)}"
    backup_sets
  end

  def backup_sets_from_backup_files(backup_files)
    backup_sets = {}
    backup_files.each do |file|
      current_basename = ::File.basename(file).sub(/(\.part\d+)?\.(bak|trn)$/i, '') # determine basename of current file
      backup_sets[current_basename] = [] if backup_sets[current_basename].nil?
      backup_sets[current_basename].push(file)
    end
    backup_sets
  end

  def get_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: false)
    if backup_folder.start_with?('\\\\')
      get_unc_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: log_only)
    else
      sql_server_backup_files(sql_server_settings, backup_basename, log_only: log_only)
    end
  end

  # Get size of uncompressed database from backup header in MB
  def get_backup_size(sql_backup_header)
    sql_backup_header['BackupSize'].to_f / 1024 / 1024
  end

  def check_header_date(sql_backup_header, backup_start_time, messages = :none) # messages options: :none, :prebackup | returns: :current, :outdated, :nobackup
    return :nobackup if sql_backup_header.nil? || sql_backup_header.empty?
    IOHelper.logger.info("Last backup for [#{sql_backup_header['DatabaseName']}] completed: #{sql_backup_header['BackupFinishDate']}") if [:prebackup, :prerestore].include?(messages)
    backup_finish_time = sql_backup_header['BackupFinishDate']
    if backup_finish_time > backup_start_time
      IOHelper.logger.info('Backup is current. Bypassing database backup.') if messages == :prebackup
      IOHelper.logger.info('Backup is current. Proceeding with restore...') if messages == :prerestore
      :current
    else
      :outdated
    end
  end

  def create_sql_login(connection_string, user, password, update_existing: false, retries: 3, retry_delay: 5)
    sid_script = ::File.read("#{sql_script_dir}/Security/GetUserSID.sql")
    raise "SQL password for login [#{user}] must not be empty!" if password.nil? || password.empty?
    values = { 'user' => user, 'password' => password }
    IOHelper.logger.info("Checking for existing SqlLogin: #{user}...")
    login_sid = execute_query(connection_string, sid_script, return_type: :scalar, values: values, readonly: true, retries: retries, retry_delay: retry_delay)
    if login_sid.nil?
      sql_script = ::File.read("#{sql_script_dir}/Security/CreateSqlLogin.sql")
      IOHelper.logger.info("Creating SqlLogin: #{user}...")
      result = execute_query(connection_string, sql_script, return_type: :first_row, values: values, retries: retries, retry_delay: retry_delay)
      raise "Failed to create SQL login: [#{user}]!" if result.nil? || result['name'].nil?
      login_sid = result['sid']
    elsif update_existing
      sql_script = ::File.read("#{sql_script_dir}/Security/UpdateSqlPassword.sql")
      IOHelper.logger.info("Login [#{user}] already exists... updating password.")
      execute_query(connection_string, sql_script, return_type: :first_row, values: values, retries: retries, retry_delay: retry_delay)
    else
      IOHelper.logger.info("Login [#{user}] already exists...")
    end
    IOHelper.logger.debug("SqlLogin [#{user}] sid: #{login_sid}")
    login_sid
  end

  def sql_login_exists?(connection_string, login)
    sid_script = ::File.read("#{sql_script_dir}/Security/GetUserSID.sql")
    raise "SQL password for login [#{login}] must not be empty!" if password.nil? || password.empty?
    values = { 'user' => login, 'password' => password }
    IOHelper.logger.info("Checking for existing SqlLogin: #{login}...")
    execute_query(connection_string, sid_script, return_type: :scalar, values: values, readonly: true, retries: retries, retry_delay: retry_delay)
  end

  def migrate_logins(start_time, source_connection_string, destination_connection_string, database_name)
    import_script_filename = export_logins(start_time, source_connection_string, database_name)
    if ::File.exist?(import_script_filename)
      IOHelper.logger.info "Importing logins on [#{connection_string_part(destination_connection_string, :server)}]..."
      execute_script_file(destination_connection_string, import_script_filename)
    else
      IOHelper.logger.warn 'Unable to migrate logins. Ensure they exist or manually create them.'
    end
  end

  # TODO: use start time to check exported file timestamp to see if it's current
  def export_logins(_start_time, connection_string, database_name)
    export_folder = Settings.paths['logins_export_folder']
    server_name = connection_string_part(connection_string, :server)
    import_script_filename = "#{export_folder}/#{FormatHelper::File.windows_friendly_name(server_name)}_#{database_name}_logins.sql"
    if SqlHelper::Database.info(connection_string, database_name)['DatabaseNotFound']
      IOHelper.logger.warn "Source database was not found. Unable to export logins#{' but import file already exists..' if ::File.exist?(import_script_filename)}."
      return import_script_filename
    end
    sql_script = ::File.read("#{sql_script_dir}/Security/GenerateCreateLoginsScript.sql")
    values = { 'databasename' => database_name }
    IOHelper.logger.info("Exporting logins associated with database: [#{database_name}] on [#{server_name}]...")
    import_script = execute_query(connection_string, sql_script, return_type: :scalar, values: values, readonly: true)
    return nil if import_script.nil? || import_script.empty?
    IOHelper.mkdir(export_folder)
    ::File.write(import_script_filename, import_script)
    import_script_filename
  end

  def validate_logins_script(connection_string, database_name)
    server_name = connection_string_part(connection_string, :server)
    sql_script = ::File.read("#{sql_script_dir}/Security/GenerateValidateLoginsScript.sql")
    values = { 'databasename' => database_name }
    IOHelper.logger.debug("Creating validate logins script for logins associated with database: [#{database_name}] on [#{server_name}]...")
    execute_query(connection_string, sql_script, return_type: :scalar, values: values, readonly: true)
  end

  def execute_script_file(connection_string, import_script_filename, values: nil, readonly: false)
    return nil if import_script_filename.nil? || import_script_filename.empty?
    sql_script = ::File.read(import_script_filename)
    execute_query(connection_string, sql_script, values: values, readonly: readonly)
  end

  def assign_database_roles(connection_string, database_name, user, roles = ['db_owner'], retries: 3, retry_delay: 5) # roles: array of database roles
    values = { 'databasename' => database_name, 'user' => user, 'databaseroles' => roles.join(',') }
    sql_script = ::File.read("#{sql_script_dir}/Security/AssignDatabaseRoles.sql")
    IOHelper.logger.info("Assigning #{roles.join(', ')} access to [#{user}] for database [#{database_name}]...")
    result = execute_query(connection_string, sql_script, return_type: :first_row, values: values, retries: retries, retry_delay: retry_delay)
    raise "Failed to assign SQL database roles for user: [#{user}]!" if result.nil? || result['name'].nil?
    result
  end

  def database_roles_assigned?(connection_string, database_name, user, roles, sid)
    values = { 'databasename' => database_name, 'user' => user, 'sid' => sid }
    validation_script = ::File.read("#{sql_script_dir}/Security/ValidateDatabaseRoles.sql")
    validation_result = execute_query(connection_string, validation_script, return_type: :first_table, values: values, readonly: true)
    roles.all? { |role| validation_result.any? { |row| row['name'].casecmp(role) == 0 } }
  end

  def connection_string_part_regex(part)
    case part
    when :server
      /(server|data source)(\s*\=\s*)([^;]+)(;)?/i
    when :database
      /(database|initial catalog)(\s*\=\s*)([^;]+)(;)?/i
    when :user # array of user/password or integrated security
      /(user id|uid)(\s*\=\s*)([^;]+)(;)?/i
    when :password
      /(password|pwd)(\s*\=\s*)([^;]+)(;)?/i
    when :integrated
      /integrated security\s*\=\s*[^;]+(;)?/i
    when :applicationintent
      /applicationintent\s*\=\s*[^;]+(;)?/i
    else
      raise "#{part} is not a supported connection string part!"
    end
  end

  def connection_string_part(connection_string, part, value_only: true) # options: :server, :database, :credentials, :readonly
    raise 'Connection string provided is nil or empty!' if connection_string.nil? || connection_string.empty?
    case part
    when :server, :database, :applicationintent
      connection_string[connection_string_part_regex(part)]
    when :user, :password
      credentials = connection_string_part(connection_string, :credentials)
      return nil if credentials.nil?
      return credentials[part]
    when :credentials # array of user/password or integrated security
      connection_string[connection_string_part_regex(:user)]
      user = Regexp.last_match(3)
      connection_string[connection_string_part_regex(:password)]
      password = Regexp.last_match(3)
      return { user: user, password: password } unless user.nil? || password.nil?
      return connection_string[connection_string_part_regex(:integrated)]
    end
    return Regexp.last_match(3) if value_only
    result = (Regexp.last_match(1) || '') + '=' + (Regexp.last_match(3) || '') + (Regexp.last_match(4) || '')
    result.empty? ? nil : result
  end

  def remove_connection_string_part(connection_string, part) # part options: :server, :database, :credentials, :applicationintent
    connection_string_new = connection_string.dup
    parts = part == :credentials ? [:user, :password, :integrated] : [part]
    parts.each { |p| connection_string_new.gsub!(connection_string_part_regex(p), '') } # unless full_part.nil? }
    connection_string_new
  end

  def hide_connection_string_password(connection_string)
    credentials = connection_string_part(connection_string, :credentials)
    credentials[:password] = credentials[:password].gsub(/(.)([^(.$)]*)(.$)/) { Regexp.last_match(1) + ('*' * Regexp.last_match(2).length) + Regexp.last_match(3) } if credentials.is_a?(Hash) && !credentials[:password].nil? && credentials[:password].length > 2
    raise "Connection string missing authentication information! Connection string: '#{connection_string}'" if credentials.nil? || credentials.empty?
    replace_connection_string_part(connection_string, :credentials, credentials)
  end

  # Supply entire value (EG: 'user id=value;password=value;' or 'integrated security=SSPI;') as the replacement_value if the part is :credentials
  # or provide a hash containing a username and password { user: 'someuser', password: 'somepassword', }
  def replace_connection_string_part(connection_string, part, replacement_value)
    FormatHelper.validate_parameters(method(__method__), binding)
    new_connection_string = remove_connection_string_part(connection_string, part)
    new_connection_string = case part
                            when :credentials
                              replacement_value = "User Id=#{replacement_value[:user]};Password=#{replacement_value[:password]}" if replacement_value.is_a?(Hash)
                              "#{new_connection_string};#{replacement_value}"
                            else
                              "#{part}=#{replacement_value};#{new_connection_string};"
                            end
    new_connection_string.gsub!(/;+/, ';')
    new_connection_string
  end

  def run_sql_as_job(connection_string, sql_script, sql_job_name, sql_job_owner = 'sa', values: nil)
    sql_script = insert_values(sql_script, values) unless values.nil?
    ensure_sql_agent_is_running(connection_string)
    job_values = { 'sqlquery' => sql_script.gsub('\'', '\'\''),
                   'jobname' => sql_job_name,
                   'jobowner' => sql_job_owner }
    sql_job_script = ::File.read("#{sql_script_dir}/Agent/CreateSQLJob.sql")
    execute_query(connection_string, sql_job_script, values: job_values)
  end

  def ensure_sql_agent_is_running(connection_string)
    sql_script = ::File.read("#{sql_script_dir}/Agent/SQLAgentStatus.sql")
    sql_server_settings = get_sql_server_settings(connection_string)
    raise "SQL Agent is not running on #{sql_server_settings['ServerName']}!" if execute_query(connection_string, sql_script, return_type: :scalar) == 0
  end

  def compress_all_tables(connection_string)
    uncompressed_count_script = ::File.read("#{sql_script_dir}/Status/UncompressedTableCount.sql")
    compress_tables_script = ::File.read("#{sql_script_dir}/Database/CompressAllTables.sql")
    IOHelper.logger.info('Checking for uncompressed tables...')
    uncompressed_count = execute_query(connection_string, uncompressed_count_script, return_type: :scalar)
    if uncompressed_count > 0
      IOHelper.logger.info("Compressing #{uncompressed_count} tables...")
      execute_query(connection_string, compress_tables_script)
      IOHelper.logger.info('Compression complete.')
    else
      IOHelper.logger.info('No uncompressed tables.')
    end
  end

  def update_sql_compatibility(connection_string, compatibility_level) # compatibility_level options: :sql_2008, :sql_2012
    sql_compatibility_script = case compatibility_level
                               when :sql_2008
                                 ::File.read("#{sql_script_dir}/Database/SetSQL2008Compatibility.sql")
                               when :sql_2012
                                 ::File.read("#{sql_script_dir}/Database/SetSQL2012Compatibility.sql")
                               else
                                 raise "Compatibility level #{compatibility_level} not yet supported!"
                               end
    IOHelper.logger.info("Ensuring SQL compatibility is set to #{compatibility_level}...")
    compatibility_result = execute_query(connection_string, sql_compatibility_script, return_type: :scalar)
    compatibility_levels =
      {
        sql_2008: 100,
        sql_2012: 110,
      }
    # IOHelper.logger.info("Compatibility level is set to #{compatibility_levels.find { |key, value| value == compatibility_result }.key}")
    IOHelper.logger.info("Compatibility level is set to #{compatibility_levels.key(compatibility_result)}")
  end

  # sql_server_settings can be for any server that has network access to these files
  def get_unc_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: false, all_time_stamps: false)
    backup_folder = FormatHelper::Directory.ensure_trailing_slash(backup_folder).tr('\\', '/')
    backup_file_extension = backup_basename.slice!(/\.(trn|bak)/i)
    backup_file_extension = log_only ? 'trn' : 'bak' if backup_file_extension.nil?
    backup_file_extension = backup_file_extension.reverse.chomp('.').reverse
    backup_files = Dir.glob("#{backup_folder}#{backup_basename}*").grep(/#{Regexp.escape(backup_basename)}(_\d{6})?(\.part\d+)?\.#{backup_file_extension}$/i)
    return [backup_files, backup_basename] if all_time_stamps
    most_recent_backup_files_and_basename(sql_server_settings, backup_files, backup_basename)
  end

  # sql_server_settings can be for any server that has network access to these files
  def backup_sets_from_unc_path(sql_server_settings, backup_folder, backup_basename, log_only: false, database_backup_header: nil, restored_database_lsn: nil)
    backup_files, backup_basename = get_unc_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: log_only, all_time_stamps: log_only)
    backup_sets = log_only ? relevant_log_backup_sets(sql_server_settings, backup_files, database_backup_header, restored_database_lsn) : { backup_basename => backup_files }
    IOHelper.logger.debug "Database backup sets found: #{JSON.pretty_generate(backup_sets)}"
    backup_sets
  end

  def backup_location_and_basename(start_time, connection_string, database_name)
    database_info = SqlHelper::Database.info(connection_string, database_name)
    server_settings = get_sql_server_settings(connection_string)
    if database_info['DatabaseNotFound']
      backup_unc_location = Settings.backups['backup_to_alternate_by_default'] ? Settings.backups['alternate_destination'] : "\\\\#{server_settings['ServerName']}\\#{Settings.backups['default_backup_share']}"
      backup_name = "#{database_name}_#{FormatHelper::DateTime.yyyymmdd(start_time)}"
      return Dir.glob("#{backup_unc_location}/#{backup_name}*".tr('\\', '/')).empty? ? [nil, nil] : [backup_unc_location, backup_name]
    end

    backup_file_path = database_info['BackupFileLocation']
    backup_file = ::File.basename(backup_file_path)
    backup_unc_location = to_unc_path(::File.dirname(backup_file_path), server_settings['ServerName'])
    backup_name = backup_basename(backup_file)
    backup_folder = if ::File.exist?("\\\\#{server_settings['ServerName']}\\#{Settings.backups['default_backup_share']}\\#{backup_file}")
                      "\\\\#{server_settings['ServerName']}\\#{Settings.backups['default_backup_share']}"
                    elsif ::File.exist?("#{backup_unc_location}/#{backup_file}")
                      backup_unc_location
                    end
    return [nil, nil] unless defined?(backup_folder)
    [backup_folder, backup_name]
  end

  # converts a path to unc_path if it contains a drive letter. Uses the server name provided
  def to_unc_path(path, server_name)
    return nil if path.nil? || path.empty? || server_name.nil? || server_name.empty?
    path.gsub(/(\p{L})+(:\\)/i) { "\\\\#{server_name}\\#{Regexp.last_match(1)}$\\" } # replace local paths with network paths
  end

  # get the basename of the backup based on a full file_path such as the SQL value from [backupmediafamily].[physical_device_name]
  def backup_basename(backup_path)
    return nil if backup_path.nil? || backup_path.empty?
    ::File.basename(backup_path).gsub(/(\.part\d+)?\.(bak|trn)/i, '')
  end

  # generates a connection string from the hash provided. Example hash: { 'server' => 'someservername', 'database' => 'somedb', 'user' => 'someuser', 'password' => 'somepass' }
  def connection_string_from_hash(connection_hash, windows_authentication: false)
    credentials = windows_authentication ? 'integrated security=SSPI;' : "user id=#{connection_hash['user']};password=#{connection_hash['password']}"
    "server=#{connection_hash['server']};database=#{connection_hash['name']};#{credentials}"
  end

  # Ensures a connection string is using integrated security instead of SQL Authentication.
  def to_integrated_security(connection_string, server_only: false)
    raise 'Failed to convert connection string to integrated security. Connection string is nil!' if connection_string.nil?
    parts = connection_string.split(';')
    new_connection_string = ''
    ommitted_parts = ['user id', 'uid', 'password', 'pwd', 'integrated security', 'trusted_connection']
    ommitted_parts += ['database', 'initial catalog'] if server_only
    parts.each { |part| new_connection_string << "#{part};" unless part.downcase.strip.start_with?(*ommitted_parts) }
    "#{new_connection_string}Integrated Security=SSPI;"
  end

  # Convert encoding of a string to utf8
  def to_utf8(text)
    text.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
  end
end
