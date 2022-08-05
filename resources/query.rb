unified_mode true if respond_to?(:unified_mode)

property :query_name, String, name_property: true
property :connection_string, String, required: true
property :sql_query, String
property :sql_queries, [String, Array], default: lazy { [sql_query] } # Provide 1 or more T-SQL queries to be executed
property :guard_query, String, default: 'SELECT 0' # Provide SELECT statement. If statement returns null, empty, or 0, guard is false and main query will be run.
property :show_output, [TrueClass, FalseClass], default: true
property :ignore_errors, [TrueClass, FalseClass], default: false

# Values to be replaced in the queries
# EG: { 'cust' => connection_id, 'database' => database_name } replaces $(cust) and $(database) in the script with their respective values.
# This is useful when using a query that comes from an artifact or when iterating through a list using the same script but different values.
property :values, Hash

default_action :run

action :run do
  new_resource.sensitive = true if connection_string.downcase.include?('password')
  raise "No query provided for Custom resource sql_helper_query[#{query_name}]" if sql_queries.all? { |q| q.nil? || q.empty? }
  updated_sql_queries = if sql_queries.is_a? String
                          Array(sql_queries)
                        else
                          sql_queries
                        end

  extend SqlHelper
  ruby_block query_name do
    block do
      raise 'Error: No SQL query provided!' if updated_sql_queries.all? { |i| i.nil? || i.empty? }
      Chef::Log.info 'Running SQL queries...'
      updated_sql_queries.each do |sql_query|
        execute_non_query(connection_string, insert_values(sql_query, values), show_output, ignore_errors)
      end
      Chef::Log.info 'Done.'
    end
    not_if do
      Chef::Log.info 'Running SQL guard query...'
      result = execute_scalar(connection_string, insert_values(guard_query, values), false)
      Chef::Log.info 'Done.'
      if result.nil? || result.empty? || result.to_s == '0'
        false
      else
        true
      end
    end
  end
end
