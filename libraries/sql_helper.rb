
# common methods for executing sql commands
module SqlHelper
  # @sql_helper_created ||= false

  # Returns row(s) affected
  def execute_non_query(connection_string, sql_query, show_output = true)
    execute_query_common(connection_string, sql_query, 'ExecuteNonQuery', show_output)
  end

  # returns single scalar value
  def execute_scalar(connection_string, sql_query, show_output = false)
    execute_query_common(connection_string, sql_query, 'ExecuteScalar', show_output)
  end

  # returns hash of columns/values for first row found in query
  def execute_reader(connection_string, sql_query, show_output = false)
    execute_query_common(connection_string, sql_query, 'ExecuteReader', show_output)
  end

  # executes a scalar or nonquery sql command - Do not call this directly from outside the module
  def execute_query_common_powershell(connection_string, sql_query, query_type, show_output)
    # create_sql_helper_script(cache_path)

    compiled_ps_sql_script = <<-EOS
            . \"#{Chef::Config[:file_cache_path]}/cookbooks/sql_helper/files/sql_helper.ps1\"

            $sqlQuery = '#{to_utf8(sql_query).gsub('\'', '\'\'')}'
            $connectionString = '#{connection_string}'
            $showOutput = $#{show_output}
            $result = #{query_type} $connectionString $sqlQuery $showOutput

            Write-Host $result

            EOS

    compiled_ps_sql_script_path = "#{::Chef::Config[:file_cache_path]}\\compiled_ps_sql_script.ps1"
    File.write(compiled_ps_sql_script_path, compiled_ps_sql_script)
    result = powershell_out("& #{compiled_ps_sql_script_path}")
    raise "Failed to execute SQL query '#{sql_query}'" if result.nil?
    raise result.stderr unless result.stderr.empty?

    process_powershell_results(result, query_type, show_output)
  end

  # Parse the results of the powershell script to extract values from other text
  def process_powershell_results(result, query_type, show_output)
    query_messages = result.stdout.strip
    Chef::Log.debug "Query output: #{query_messages}"
    query_results = query_messages.slice!(/\[\s*\{[\S\s]*\}\s*\]/) # Separate array of hashes from other output
    raise query_messages if %w(error exception).any? { |w| query_messages.downcase.include?(w) }
    Chef::Log.debug "Query results: \n#{query_results}\n"
    Chef::Log.debug "Query messages: \n#{query_messages}\n" unless show_output
    Chef::Log.info "Query messages: \n#{query_messages}\n" if show_output

    case query_type
    when 'ExecuteReader'
      return [] if query_results.nil?
      evaluate_sql_results(query_results)
    else
      return nil if query_results.nil?
      evaluated_result = evaluate_sql_results(query_results)
      return nil if evaluated_result.nil? || evaluated_result.empty?
      evaluated_result.first['result'] # Return a scalar value or rows affected
    end
  end

  # Evaluate the extracted values from the powershell results
  def evaluate_sql_results(query_results)
    formatted_results = query_results.strip.gsub('\\') { '\\\\' }
    Chef::Log.debug "evaluate_sql_results: #{formatted_results}"
    begin
      # rubocop:disable Lint/Eval
      eval(formatted_results) # TODO: Find safer way to convert results to hash - might require doing sql connection in ruby
      # rubocop:enable Lint/Eval
    rescue
      raise 'Unable to evaluate the SQL results - the format was invalid.'
    end
  end

  # executes a scalar or nonquery sql command - Do not call this directly from outside the module
  # TODO: Add linux support
  def execute_query_common(connection_string, sql_query, query_type, show_output = nil)
    execute_query_common_powershell(connection_string, sql_query, query_type, show_output)
  end

  def get_sql_server_settings(connection_string)
    extend WindowsConfiguration
    get_sql_settings_script = ::File.read("#{Chef::Config['file_cache_path']}/cookbooks/sql_helper/files/GetSQLSettings.sql")
    sql_server_settings = execute_reader(connection_string, get_sql_settings_script, false).first
    Chef::Log.debug "sql_server_settings: #{sql_server_settings}"
    Chef::Log.debug "windows authentication: #{%w(pwd password).any? { |pwd| connection_string.include?(pwd) }}"
    return nil if sql_server_settings.nil? || sql_server_settings['ServerName'].nil?
    direct_connection_string = connection_string.gsub(sql_server_settings['DataSource'], sql_server_settings['ServerName'])
    connection_string.gsub!(sql_server_settings['ServerName'], sql_server_settings['DataSource'])
    sql_server_settings['direct_connection_string'] = direct_connection_string # Does not use AlwaysOn listener
    sql_server_settings['connection_string'] = connection_string
    sql_server_settings['DataDir'] = ensure_closing_slash(sql_server_settings['DataDir'])
    sql_server_settings['LogDir'] = ensure_closing_slash(sql_server_settings['LogDir'])
    sql_server_settings['BackupDir'] = ensure_closing_slash(sql_server_settings['BackupDir'])
    sql_server_settings
  end

  def get_backup_sql_server_settings(connection_string)
    sql_server_settings = get_sql_server_settings(connection_string)
    sql_server_settings = get_sql_server_settings(to_integrated_security(connection_string)) if sql_server_settings.nil? || sql_server_settings['BackupDir'] == 'null'
    raise "FATAL: Current user #{ENV['user'] || ENV['username']} does not have access to backup database!" if sql_server_settings.nil? || sql_server_settings['BackupDir'] == 'null'
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
    backup_files.each { |backup_file| result << " DISK = N''#{backup_file}''," }
    result.chomp(',')
  end

  # Returns the database size in MB
  def get_database_size(connection_string, database_name)
    sql_script = "SELECT SUM(size)/128.0 FROM [#{database_name}].[sys].[sysfiles]"
    execute_scalar(connection_string, sql_script).to_f
  end

  # Returns a hash with the following fields: Available_MB, Total_MB, Percent_Free
  def get_sql_disk_space(connection_string, target_folder)
    sql_script = ::File.read("#{Chef::Config['file_cache_path']}/cookbooks/sql_helper/files/GetDiskSpace.sql")
    sql_script = insert_values(sql_script, 'targetfolder' => target_folder)
    execute_reader(connection_string, sql_script).first
  end

  # Returns the headers from the backup set provided. Pass an array of path strings to the backup files.
  def get_sql_backup_headers(connection_string, backup_files)
    disk_backup_files = backup_fileset_names(backup_files)
    sql_script = ::File.read("#{Chef::Config['file_cache_path']}/cookbooks/sql_helper/files/GetBackupHeaders.sql")
    sql_script = insert_values(sql_script, 'bkupfiles' => disk_backup_files)
    execute_reader(connection_string, sql_script)
  end

  def sql_server_backup_files(sql_server_settings, base_backup_name)
    values = { 'targetfolder' => sql_server_settings['BackupDir'],
               'bkupname' => base_backup_name }
    sql_script = ::File.read("#{Chef::Config['file_cache_path']}/cookbooks/sql_helper/files/GetBackupFiles.sql")
    sql_script = insert_values(sql_script, values)
    backup_files_results = execute_reader(sql_server_settings['connection_string'], sql_script)
    backup_files = []
    backup_files_results.each do |file|
      backup_files.push("#{sql_server_settings['BackupDir']}#{file['FileName']}")
    end
    backup_files
  end

  # Returns a query to perform a database backup.
  def run_sql_backup(connection_string, backup_folder, database_name, base_backup_name, compression)
    backup_status_script = ::File.read("#{Chef::Config['file_cache_path']}/cookbooks/sql_helper/files/BackupProgress.sql")
    return unless execute_reader(connection_string, backup_status_script).empty? # Return if a backup is in progress

    values = { 'bkupdbname' => database_name,
               'bkupname' => base_backup_name,
               'compressbackup' => compression,
               'bkupdestdir' => backup_folder }
    sql_backup_script = ::File.read("#{Chef::Config['file_cache_path']}/cookbooks/sql_helper/files/BackupDatabase.sql")
    sql_backup_script = insert_values(sql_backup_script, values)
    Chef::Log.info "Backing up to: #{backup_folder}..."
    run_sql_as_job(connection_string, sql_backup_script, "Backup: #{base_backup_name}")
  end

  def run_sql_as_job(connection_string, sql_script, sql_job_name, sql_job_owner = 'sa')
    ensure_sql_agent_is_running(connection_string)
    values = { 'sqlquery' => sql_script.gsub('\'', '\'\''),
               'jobname' => sql_job_name,
               'jobowner' => sql_job_owner }
    sql_job_script = ::File.read("#{Chef::Config['file_cache_path']}/cookbooks/sql_helper/files/CreateSQLJob.sql")
    sql_job_script = insert_values(sql_job_script, values)
    execute_non_query(connection_string, sql_job_script)
  end

  def ensure_sql_agent_is_running(connection_string)
    sql_script = <<-EOS
      IF EXISTS (  SELECT 1 FROM master.dbo.sysprocesses WHERE program_name = N'SQLAgent - Generic Refresher')
      BEGIN
        SELECT 1 AS 'SQLServerAgentRunning'
      END
      ELSE
      BEGIN
        SELECT 0 AS 'SQLServerAgentRunning'
      END
    EOS

    sql_server_settings = get_sql_server_settings(connection_string)
    raise "SQL Agent is not running on #{sql_server_settings['DataSource']}!" if execute_scalar(connection_string, sql_script).strip == '0'
  end

  # Ensures a connection string is using integrated security instead of SQL Authentication.
  def to_integrated_security(connection_string)
    parts = connection_string.split(';')
    new_connection_string = ''
    sql_auth_parts = ['user id', 'uid', 'password', 'pwd', 'integrated security', 'trusted_connection']
    parts.each { |part| new_connection_string << "#{part};" unless part.downcase.start_with?(*sql_auth_parts) }
    "#{new_connection_string}Integrated Security=true;"
  end

  # # Create sql helper powershell script
  # def create_sql_helper_script(cache_path)
  #   return if @sql_helper_created
  #
  #   directory cache_path do
  #     action :create
  #     recursive true
  #   end.run_action(:create)
  #
  #   cookbook_file "#{cache_path}/sql_helper.ps1" do
  #     action :create
  #     cookbook 'platform'
  #     source 'sql_helper.ps1'
  #   end.run_action(:create)
  #
  #   @sql_helper_created = true
  # end

  # Convert encoding of a string to utf8
  def to_utf8(text)
    text.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
  end
end
