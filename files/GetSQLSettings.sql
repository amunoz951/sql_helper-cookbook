SET NOCOUNT ON

DECLARE @datafiledir nvarchar(255)
DECLARE @logfiledir nvarchar(255)
DECLARE @bkupfiledir nvarchar(255)
DECLARE @sqlcompression bit

-- Data files section
select Top (1) @datafiledir = reverse(substring(reverse(physical_name), charindex('\', reverse(physical_name)),LEN(physical_name) -1)) from sys.master_files
where physical_name like '%sqldata%' and type_desc = 'ROWS'

select Top (1) @logfiledir = reverse(substring(reverse(physical_name), charindex('\', reverse(physical_name)),LEN(physical_name) -1)) from sys.master_files
where physical_name like '%sqllog%' AND type_desc = 'LOG'

SET @bkupfiledir = LEFT(@datafiledir, LEN(@datafiledir)-5) + 'backup\'

-- Compression section
DECLARE @CompressionValue sql_variant

SELECT @CompressionValue = value
FROM sys.configurations
WHERE name = 'backup compression default'

-- Distributor section
DECLARE @distributor_name NVARCHAR(255)

SELECT @distributor_name = datasource from master.dbo.sysservers where srvname = 'repl_distributor'

-- Get AlwaysOn listener
DECLARE @AlwaysOnCmd NVARCHAR(MAX)
DECLARE @ParmDefinition NVARCHAR(MAX)
DECLARE @AlwaysOnEnabled bit
DECLARE @DataSource nvarchar(256)

SET @AlwaysOnCmd =
'DECLARE @AGListenerName NVARCHAR(100)
DECLARE @AGListenerPort NVARCHAR(5)

select @AGListenerName = dns_name, @AGListenerPort = CONVERT(NVARCHAR,port) from sys.availability_group_listeners AL
INNER JOIN master.sys.availability_groups AS AG
ON AL.group_id = AG.group_id
INNER JOIN master.sys.availability_replicas AS AR
    ON AG.group_id = AR.group_id
INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
    ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1 and arstates.role = 1 -- Role 1 = Primary

IF @@ROWCOUNT > 0
BEGIN
	SELECT @AlwaysOnEnabledOUT = 1,
	 			 @DataSourceOUT = @AGListenerName + '','' + @AGListenerPort
END
ELSE
BEGIN
	SELECT @AlwaysOnEnabledOUT = 0,
				 @DataSourceOUT = @@SERVERNAME
END
'

SET @ParmDefinition =
'@AlwaysOnEnabledOUT bit OUTPUT,
@DataSourceOUT nvarchar(256) OUTPUT
'

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[sys].[availability_group_listeners]'))
BEGIN
 exec sp_executesql
	 @AlwaysOnCmd,
	 @ParmDefinition,
	 @AlwaysOnEnabledOUT = @AlwaysOnEnabled OUTPUT,
	 @DataSourceOUT = @DataSource OUTPUT
END

SELECT @datafiledir AS [DataDir], @logfiledir AS [LogDir], @bkupfiledir AS [BackupDir],
 COALESCE(@CompressionValue, 0) AS [CompressBackup], COALESCE(@distributor_name, 'none') AS [Distributor],
 COALESCE(@AlwaysOnEnabled, 0) AS [AlwaysOnEnabled], COALESCE(@DataSource, @@SERVERNAME) AS [DataSource]
