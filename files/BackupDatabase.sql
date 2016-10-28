SET NOCOUNT ON

DECLARE @database_name nvarchar(max)
DEClARE @backup_name nvarchar(max)
DECLARE @database_size int
DECLARE @disk_files nvarchar(max)
DECLARE @backup_location nvarchar(max)
DECLARE @file_counter int
DECLARE @size_increment int
DECLARE @i int
DECLARE @compression nvarchar(1)
DECLARE @compression_text nvarchar(50)
DECLARE @sql nvarchar(max)

SET @database_name = '$(bkupdbname)'
SET @backup_name = '$(bkupname)'
SET @compression = '$(compressbackup)'
SET @backup_location = '$(bkupdestdir)'
SET @file_counter = 1

IF (@compression = 'true')
BEGIN
	SET @compression_text = ' COMPRESSION,'
END
ELSE
BEGIN
	SET @compression_text = ''
END

IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = @database_name)
BEGIN
	PRINT ('Error: [' + @database_name + '] does not exist on [' + CONVERT(nvarchar,SERVERPROPERTY('ServerName')) + ']!')
	RETURN
END

SELECT @database_size = CAST(SUM(size) * 8. / 1024 AS DECIMAL(8,2))
FROM sys.master_files WITH(NOWAIT)
WHERE database_id = DB_ID(@database_name) -- for current db
GROUP BY database_id

SET @size_increment = 58800
SET @i = @size_increment

WHILE (@i < @database_size)
BEGIN
	IF @disk_files is null SET @disk_files = ' DISK = N''' + @backup_location + '\' + @backup_name + '.part1.bak'''
	SET @file_counter = @file_counter + 1
	SET @disk_files = @disk_files + ',  DISK = N''' + @backup_location + '\' + @backup_name + '.part' + CONVERT(nvarchar(2),@file_counter) + '.bak'''
	SET @i = @i + @size_increment
END

IF (@file_counter = 1)
BEGIN
	SET @disk_files = ' DISK = N''' + @backup_location + '\' + @backup_name + '.bak'''
END

SET @sql = 'BACKUP DATABASE [' + @database_name + '] TO  ' + @disk_files + ' WITH' + @compression_text + ' NOFORMAT, COPY_ONLY, NOINIT,  NAME = N''' + @database_name + '-Full Database Backup'', SKIP, NOREWIND, NOUNLOAD,  STATS = 10'

PRINT ('')
PRINT ('Starting backup of [' + @database_name + ']...')
PRINT ('')

EXEC (@sql)
