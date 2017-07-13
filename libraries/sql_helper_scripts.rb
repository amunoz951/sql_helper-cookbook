module SqlHelper
  def self.powershell_functions
    <<-EOS
      function Invoke-SQL {
          param(
              [string] $connectionString = $(throw "Please specify a connection string"),
              [string] $sqlCommand = $(throw "Please specify a query.")
            )

          $connection = new-object system.data.SqlClient.SQLConnection($connectionString)
          $command = new-object system.data.sqlclient.sqlcommand($sqlCommand,$connection)
          $connection.Open()

          $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
          $dataset = New-Object System.Data.DataSet
          $adapter.Fill($dataSet) | Out-Null

          $connection.Close()
          $dataSet.Tables

      }

      function ConvertSqlDatasetTo-Json {
          param(
              [object] $dataset = $(throw "Please specify a dataset")
            )

          $convertedTables = '{ '
          foreach ($table in $dataset.DataSet.Tables) {
            $convertedTable = (($table | select $table.Columns.ColumnName) | ConvertTo-Json -Compress).Trim()
            if (!$convertedTable.StartsWith('[')) { $convertedTable = "[ $convertedTable ]" } # Convert to Array if it's not
            $convertedTables += '"' + $table.TableName + '": ' + $convertedTable + ','
          }
          $convertedTables.TrimEnd(',') + ' }'
      }

      function Build-ConnectionString{
          param(
              [string] $vip = $(throw "Please specify a server vip"),
              [int] $port,
              [string] $database = $(throw "Please specify a database"),
              [string] $username,
              [string] $password
          )
          $target = $vip
          if($port -ne $null -and $port -ne 0){
              $target = "$target,$port"
          }
          if(($username -ne "") -and ($password -ne "")){
              $credentials = "Integrated Security=False;User ID=$username;Password=$password"
          }
          else{
              Write-Warning "no credentials provided.  falling back to integrated security"
              $credentials = "Integrated Security=True;"
          }
          return "Data Source=$target; Initial Catalog=$database; $credentials"
      }
    EOS
  end
end
