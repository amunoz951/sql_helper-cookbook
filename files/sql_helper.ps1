# PowerShell v2.0 compatible version of [string]::IsNullOrWhitespace.
function StringIsNullOrWhitespace([string] $string)
{
    if ($string -ne $null) { $string = $string.Trim() }
    return [string]::IsNullOrEmpty($string)
}

# Returns row(s) affected
function ExecuteNonQuery {
param(
    [Parameter(Mandatory=$True,Position=1)] [string]$cnstr,
    [Parameter(Mandatory=$True,Position=2)] [string]$query,
    [Parameter(Mandatory=$False,Position=3)] [bool]$showOutput = $true
)
  return ExecuteQuery $cnstr $query 'nonquery' $showOutput
}

# Returns scalar value
function ExecuteScalar {
param(
    [Parameter(Mandatory=$True,Position=1)] [string]$cnstr,
    [Parameter(Mandatory=$True,Position=2)] [string]$query,
    [Parameter(Mandatory=$False,Position=3)] [bool]$showOutput = $true
)
  return ExecuteQuery $cnstr $query 'scalar' $showOutput
}

# Returns first row found in results
function ExecuteReader {
param(
    [Parameter(Mandatory=$True,Position=1)] [string]$cnstr,
    [Parameter(Mandatory=$True,Position=2)] [string]$query,
    [Parameter(Mandatory=$False,Position=3)] [bool]$showOutput = $true
)
  return ExecuteQuery $cnstr $query 'reader' $showOutput
}

# Returns scalar value or row(s) affected
function ExecuteQuery {
param(
    [Parameter(Mandatory=$True,Position=1)] [string]$cnstr,
    [Parameter(Mandatory=$True,Position=2)] [string]$query,
    [Parameter(Mandatory=$True,Position=3)] [string]$queryType,
    [Parameter(Mandatory=$False,Position=4)] [bool]$showOutput = $true
)
  $Connection = New-Object System.Data.SQLClient.SQLConnection
  $Connection.ConnectionString = $cnstr

  # Attach the InfoMessage Event Handler to the connection
  $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {param($sender, $event) Write-Host $event.Message };
  $Connection.add_InfoMessage($handler);
  $Connection.FireInfoMessageEventOnUserErrors = $true;

  # Let chef raise any exceptions
  $connection.Open()
  $queries = $query -split "\n\s*GO\s*\n" # Split the query on each GO statement.
  $command = New-Object System.Data.SQLClient.SQLCommand
  $command.Connection = $connection
  foreach ($currentQuery in $queries) {
    $command.CommandText = $currentQuery
    if (StringIsNullOrWhitespace($command.CommandText)) { continue }
    if ($queryType -eq 'scalar') {
      $result = '[ { ''result'' => ''' + $command.ExecuteScalar() + ''' } ]'
    } elseif ($queryType -eq 'reader') {
      $reader = $command.ExecuteReader()
      $result = "[ "
      while ($reader.Read()) {
        $result = "$result { "
        for ($i = 0;$i -lt $reader.FieldCount; $i++) {
          $columnName = $reader.GetName($i)
          if ($reader.IsDBNull($i)) {
            $columnValue = 'null'
          } else {
            $columnValue = $reader.GetValue($i)
          }
          $result = "$result '$columnName' => '$columnValue',"
        }
        $result = $result.Substring(0, $result.Length - 1)
        $result = "$result },"
      }
      $result = $result.Substring(0, $result.Length - 1)
      $result = "$result ]"
    } else {
      $result = '[ { ''result'' => ''' + $command.ExecuteNonQuery() + ''' } ]'
    }
  }
  $Connection.Close()

  return $result
}
