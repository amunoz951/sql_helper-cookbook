require 'JSON'
require 'pp'
require 'English'

module SqlHelper
  def sql_script_dir
    "#{File.dirname(__FILE__)}/sql"
  end

  extend IOHelper
  extend Settings

  def execute_sql(connection_string, sql_query)
    ps_script = <<-EOS.strip
        . "#{Settings.paths['script_dir']}\\sql_helper.ps1"

        $connectionString = '#{connection_string}'
        $sqlCommand = '#{sql_query.gsub('\'', '\'\'')}'
        Invoke-SQL -connectionString $connectionString -sqlCommand $sqlCommand
      EOS

    puts IO.popen(['powershell.exe', ps_script], &:read).strip # run sql
  end

  # Execute a SQL query and return a dataset, table, row, or scalar, based on the return_type specified
  #   return_type: :all_tables, :first_table, :first_row, :scalar
  #   values: hash of values to replace sqlcmd style variables.
  #           EG: sql_query = SELECT * FROM $(databasename)
  #               values = { 'databasename' => 'my_database_name' }
  #   readonly: if true, sets the connection_string to readonly (Useful with AOAG)
  def execute_query(connection_string, sql_query, return_type: :all_tables, values: nil, readonly: false, ignore_missing_values: false)
    sql_query = insert_values(sql_query, values) unless values.nil?
    missing_values = sql_query.scan(/(\$\([a-zA-Z]+\))/)
    raise "sql_query has missing variables! Ensure that values are supplied for: #{missing_values.uniq.join(', ')}\n" unless missing_values.nil? || missing_values.empty? || ignore_missing_values
    connection_string_updated = connection_string.dup
    connection_string_updated = replace_connection_string_part(connection_string, :applicationintent, 'readonly') if readonly
    IOHelper.logger.debug("Executing query with connection_string: \n\t#{hide_connection_string_password(connection_string_updated)}")
    IOHelper.logger.debug(show_queries(sql_query))
    ps_script = <<-EOS.strip
        . "#{Settings.paths['script_dir']}\\sql_helper.ps1"

        $connectionString = '#{connection_string_updated}'
        $sqlCommand = '#{sql_query.gsub('\'', '\'\'')}'
        $dataset = Invoke-SQL -connectionString $connectionString -sqlCommand $sqlCommand
        ConvertSqlDatasetTo-Json -dataset $dataset
      EOS

    ps_script_file = "#{Settings.paths['cache_path']}/scripts/ps_script-thread_id-#{Thread.current.object_id}.ps1"
    FileUtils.mkdir_p ::File.dirname(ps_script_file)
    ::File.write(ps_script_file, ps_script)
    stdout, stderr, status = Open3.capture3('powershell.exe', "powershell -File \"#{ps_script_file}\"")

    unless stderr.strip.empty? && status.exitstatus == 0
      IOHelper.logger.fatal('Exception executing sql query!' \
        "\n#{'-' * 114}\nexit code: #{status.exitstatus}\n#{stderr}\nConnectionString: '#{hide_connection_string_password(connection_string)}'\n#{'=' * 114}\n")
    end
    convert_powershell_tables_to_hash(stdout, return_type)
  end

  def show_queries(sql_query)
    ps_script =
      <<-EOS
        . "#{Settings.paths['script_dir']}\\sql_helper.ps1"
        $sqlCommand = '#{sql_query.gsub('\'', '\'\'')}'
        $sqlCommands = $sqlCommand -split "\n\s*GO\s*\n" # Split the query on each GO statement.
        foreach ($sqlcmd in $sqlCommands) {
          write-host "===== Sql command: =====\n$sqlcmd\n========= End of Sql command ========="
        }
      EOS

    ps_script_file = "#{Settings.paths['cache_path']}/scripts/ps_script-thread_id-#{Thread.current.object_id}.ps1"
    FileUtils.mkdir_p ::File.dirname(ps_script_file)
    ::File.write(ps_script_file, ps_script)
    result = IO.popen(['powershell.exe', "powershell -File \"#{ps_script_file}\""], &:read).strip # run sql

    result
  end

  def convert_powershell_tables_to_hash(json_string, return_type = :all_tables) # options: :all_tables, :first_table, :first_row
    IOHelper.logger.debug(json_string)
    result_hash = if json_string.empty?
                    {}
                  else
                    convert_javascript_dates_to_time_objects(JSON.parse(to_utf8(json_string.sub(/[^{\[]*/, ''))))
                  end
    case return_type
    when :first_table
      result_hash.values.first
    when :first_row
      raise 'No tables were returned by specified sql query' if result_hash.values.first.nil?
      result_hash.values.first.first
    when :scalar
      return nil if result_hash.values.first.nil? || result_hash.values.first.first.nil?
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

  def connection_string_valid?(connection_string, suppress_failure: true)
    sql_script = 'SELECT @@SERVERNAME AS [ServerName]'
    !execute_query(connection_string, sql_script, return_type: :scalar).nil?
  rescue
    raise unless suppress_failure
    false
  end

  def get_sql_server_settings(connection_string)
    get_sql_settings_script = ::File.read("#{sql_script_dir}/Status/SQLSettings.sql")
    sql_server_settings = execute_query(SqlHelper.remove_connection_string_part(connection_string, :database), get_sql_settings_script, return_type: :first_row)
    # TODO: determine why pretty_inspect is crashing: IOHelper.logger.debug "sql_server_settings: \n#{sql_server_settings.pretty_inspect}"
    return nil if sql_server_settings.nil? || sql_server_settings['ServerName'].nil?

    direct_connection_string = connection_string.gsub(sql_server_settings['DataSource'], sql_server_settings['ServerName'])
    application_connection_string = connection_string.gsub(sql_server_settings['ServerName'], sql_server_settings['DataSource'])
    sql_server_settings['direct_connection_string'] = direct_connection_string # Does not use AlwaysOn listener
    sql_server_settings['connection_string'] = application_connection_string
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
  def insert_values(sql_query, values)
    return sql_query if values.nil? || values.all? { |i, _j| i.nil? || i.empty? }

    sql_query = to_utf8(sql_query)
    values.each do |key, value|
      sql_query.gsub!("$(#{key})") { value }
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
    raise 'connection_string is a required argument for get_sql_backup_headers!' if connection_string.nil? || connection_string.empty?
    raise 'backup_files is a required argument for get_sql_backup_headers!' if backup_files.nil? || backup_files.empty?
    disk_backup_files = backup_fileset_names(backup_files)
    sql_script = ::File.read("#{sql_script_dir}/Database/GetBackupHeaders.sql")
    execute_query(connection_string, sql_script, return_type: :first_table, values: { 'bkupfiles' => disk_backup_files })
  end

  def sql_server_backup_files(sql_server_settings, backup_basename)
    values = { 'targetfolder' => sql_server_settings['BackupDir'],
               'bkupname' => backup_basename }
    sql_script = ::File.read("#{sql_script_dir}/Database/GetBackupFiles.sql")
    backup_files_results = execute_query(sql_server_settings['connection_string'], sql_script, return_type: :first_table, values: values)
    backup_files = []
    backup_files_results.each do |file|
      backup_files.push("#{FormatHelper::Directory.ensure_trailing_slash(sql_server_settings['BackupDir'])}#{file['FileName']}")
    end
    backup_files
  end

  def get_backup_files(sql_server_settings, backup_folder, backup_basename)
    if backup_folder.start_with?('\\\\')
      get_unc_backup_files(backup_folder, backup_basename)
    else
      sql_server_backup_files(sql_server_settings, backup_basename)
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

  def create_sql_login(connection_string, user, password)
    sid_script = ::File.read("#{sql_script_dir}/Security/GetUserSID.sql")
    raise "SQL password for login [#{user}] must not be empty!" if password.nil? || password.empty?
    values = { 'user' => user, 'password' => password }
    IOHelper.logger.info("Checking for existing SqlLogin: #{user}...")
    login_sid = execute_query(connection_string, sid_script, return_type: :scalar, values: values, readonly: true)
    if login_sid.nil?
      sql_script = ::File.read("#{sql_script_dir}/Security/CreateSqlLogin.sql")
      IOHelper.logger.info("Creating SqlLogin: #{user}...")
      result = execute_query(connection_string, sql_script, return_type: :first_row, values: values)
      raise "Failed to create SQL login: [#{user}]!" if result.nil? || result['name'].nil?
      login_sid = result['sid']
    else
      sql_script = ::File.read("#{sql_script_dir}/Security/UpdateSqlPassword.sql")
      IOHelper.logger.info("Login [#{user}] already exists... updating password.")
      execute_query(connection_string, sql_script, return_type: :first_row, values: values)
    end
    IOHelper.logger.debug("SqlLogin [#{user}] sid: #{login_sid}")
    login_sid
  end

  def export_logins(connection_string, database_name)
    export_folder = Settings.paths['logins_export_folder']
    server_name = connection_string_part(connection_string, :server)
    sql_script = ::File.read("#{sql_script_dir}/Security/CreateLoginsMigrationScript.sql")
    values = { 'databasename' => database_name }
    IOHelper.logger.info("Exporting logins associated with database: [#{database_name}] on [#{server_name}]...")
    import_script = execute_query(connection_string, sql_script, return_type: :scalar, values: values, readonly: true)
    return nil if import_script.nil? || import_script.empty?
    IOHelper.mkdir(export_folder)
    import_script_filename = "#{export_folder}/#{FormatHelper::File.windows_friendly_name(server_name)}_#{database_name}_logins_#{FormatHelper::DateTime.yyyymmdd}.sql"
    ::File.write(import_script_filename, import_script)
    import_script_filename
  end

  def execute_script_file(connection_string, import_script_filename, values: nil, readonly: false)
    return nil if import_script_filename.nil? || import_script_filename.empty?
    sql_script = ::File.read(import_script_filename)
    execute_query(connection_string, sql_script, values: values, readonly: readonly)
  end

  def assign_database_roles(connection_string, database_name, user, roles = ['db_owner']) # roles: array of database roles
    values = { 'databasename' => database_name, 'user' => user, 'databaseroles' => roles.join(',') }
    sql_script = ::File.read("#{sql_script_dir}/Security/AssignDatabaseRoles.sql")
    IOHelper.logger.info("Assigning #{roles.join(', ')} access to [#{user}] for database [#{database_name}]...")
    result = execute_query(connection_string, sql_script, return_type: :first_row, values: values)
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
    case part
    when :server, :database, :applicationintent
      connection_string[connection_string_part_regex(part)]
    when :user, :password
      credentials = connection_string_part(connection_string, :credentials)
      return nil if credentials.nil?
      return credentials[part]
    when :credentials # array of user/password or integrated security
      user = connection_string.slice(connection_string_part_regex(:user), 3)
      password = connection_string.slice(connection_string_part_regex(:password), 3)
      return { user: user, password: password } unless user.nil? || password.nil?
      return connection_string[connection_string_part_regex(:integrated)]
    end
    return Regexp.last_match[3] if value_only
    result = (Regexp.last_match[1] || '') + '=' + (Regexp.last_match[3] || '') + (Regexp.last_match[4] || '')
    IOHelper.logger.debug("connection_string_part: #{result} \n caller: #{caller.select { |method| method[%r{(embedded/)(lib|bin)}].nil? }.pretty_inspect}")
    result.empty? ? nil : result
  end

  def remove_connection_string_part(connection_string, part) # part options: :server, :database, :credentials, :applicationintent
    connection_string_new = connection_string.dup
    parts = part == :credentials ? [:user, :password, :integrated] : [part]
    parts.each { |p| connection_string_new.gsub!(connection_string_part_regex(p), '') } # unless full_part.nil? }
    IOHelper.logger.debug("remove_connection_string_part:  part removed: #{part} / new connection_string: #{connection_string_new}")
    connection_string_new
  end

  def hide_connection_string_password(connection_string)
    credentials = connection_string_part(connection_string, :credentials)
    credentials[:password] = '********' if credentials.is_a?(Hash) && !credentials[:password].nil? && !credentials[:password].empty?
    raise "Connection string missing authentication information! Connection string: '#{connection_string}'" if credentials.nil? || credentials.empty?
    replace_connection_string_part(connection_string, :credentials, credentials)
  end

  # Supply entire value (EG: 'user id=value;password=value;' or 'integrated security=SSPI;') as the replacement_value if the part is :credentials
  # or provide a hash containing a username and password { user: 'someuser', password: 'somepassword', }
  def replace_connection_string_part(connection_string, part, replacement_value)
    raise 'connection_string is a required argument for replace_connection_string_part!' if connection_string.nil?
    raise 'part is a required argument for replace_connection_string_part!' if part.nil?
    raise 'replacement_value is a required argument for replace_connection_string_part!' if replacement_value.nil?
    new_connection_string = remove_connection_string_part(connection_string, part)
    new_connection_string = case part
                            when :credentials
                              replacement_value = "User Id=#{replacement_value[:user]};Password=#{replacement_value[:password]}" if replacement_value.is_a?(Hash)
                              "#{new_connection_string};#{replacement_value}"
                            else
                              IOHelper.logger.debug("replace_connection_string_part - new value: #{replacement_value}")
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
                                 IOHelper.logger.fatal("Compatibility level #{compatibility_level} not yet supported!")
                               end
    IOHelper.logger.info("Ensuring SQL compatibility is set to #{compatibility_level}...")
    compatibility_result = execute_query(connection_string, sql_compatibility_script, return_type: :scalar)
    compatibility_levels =
      {
        sql_2008: 100,
        sql_2012: 110,
      }
    IOHelper.logger.info("Compatibility level is set to #{compatibility_levels.key(compatibility_result)}")
  end

  def get_unc_backup_files(backup_folder, backup_basename)
    backup_folder = FormatHelper::Directory.ensure_trailing_slash(backup_folder)
    backup_files = if ::File.exist?("#{backup_folder}#{backup_basename}.bak")
                     ["#{backup_folder}#{backup_basename}.bak"]
                   else
                     Dir.glob("#{backup_folder}#{backup_basename}.part*.bak")
                   end
    backup_files
  end

  def backup_location_and_basename(connection_string, database_name)
    database_info = SqlHelper::Database.info(connection_string, database_name)
    server_settings = get_sql_server_settings(connection_string)

    backup_file = ::File.basename(database_info['BackupFileLocation'])
    backup_name = backup_basename(backup_file)
    backup_folder = if ::File.exist?("\\\\#{server_settings['ServerName']}\\#{Settings.backups['default_backup_share']}\\#{backup_file}")
                      "\\\\#{server_settings['ServerName']}\\#{Settings.backups['default_backup_share']}"
                    else
                      to_unc_path(::File.dirname(database_info['BackupFileLocation']), server_settings['ServerName'])
                    end
    [backup_folder, backup_name]
  end

  # converts a path to unc_path if it contains a drive letter. Uses the server name provided
  def to_unc_path(path, server_name)
    return nil if path.nil? || path.empty? || server_name.nil? || server_name.empty?
    path.gsub(/(\p{L})+(:\\)/i, "\\\\#{server_name}\\\\1$\\") # replace local paths with network paths
  end

  # get the basename of the backup based on a full file_path such as the SQL value from [backupmediafamily].[physical_device_name]
  def backup_basename(backup_path)
    return nil if backup_path.nil? || backup_path.empty?
    ::File.basename(backup_path).gsub(/(\.part\d+)?\.bak/i, '')
  end

  # Ensures a connection string is using integrated security instead of SQL Authentication.
  def to_integrated_security(connection_string, server_only: false)
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
