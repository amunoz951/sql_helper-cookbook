SET NOCOUNT ON

DECLARE @bkup_files nvarchar(max)
DECLARE @rowcount int

DECLARE @HeaderOnly_cmd nvarchar(max)
DECLARE @BackupSetPosition int
DECLARE @Version14Plus bit
DECLARE @SQLMajorVersion nvarchar(128)

SET @bkup_files = '$(bkupfiles)'

SET @SQLMajorVersion = CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductVersion'))
SET @SQLMajorVersion = LEFT(@SQLMajorVersion, CHARINDEX('.', @SQLMajorVersion) - 1)

SET @HeaderOnly_cmd =
'DECLARE @BackupSets TABLE(
	BackupName nvarchar(128),
    BackupDescription nvarchar(255),
    BackupType smallint,
    ExpirationDate datetime,
    Compressed bit,
    Position smallint,
    DeviceType tinyint,
    UserName nvarchar(128),
    ServerName nvarchar(128),
    DatabaseName nvarchar(128),
    DatabaseVersion int,
    DatabaseCreationDate datetime,
    BackupSize numeric(20,0),
    FirstLSN numeric(25,0),
    LastLSN numeric(25,0),
    CheckpointLSN numeric(25,0),
    DatabaseBackupLSN numeric(25,0),
    BackupStartDate datetime,
    BackupFinishDate datetime,
    SortOrder smallint,
    CodePage smallint,
    UnicodeLocaleId int,
    UnicodeComparisonStyle int,
    CompatibilityLevel tinyint,
    SoftwareVendorId int,
    SoftwareVersionMajor int,
    SoftwareVersionMinor int,
    SoftwareVersionBuild int,
    MachineName nvarchar(128),
    Flags int,
    BindingID uniqueidentifier,
    RecoveryForkID uniqueidentifier,
    Collation nvarchar(128),
    FamilyGUID uniqueidentifier,
    HasBulkLoggedData bit,
    IsSnapshot bit,
    IsReadOnly bit,
    IsSingleUser bit,
    HasBackupChecksums bit,
    IsDamaged bit,
    BeginsLogChain bit,
    HasIncompleteMetaData bit,
    IsForceOffline bit,
    IsCopyOnly bit,
    FirstRecoveryForkID uniqueidentifier,
    ForkPointLSN numeric(25,0),
    RecoveryModel nvarchar(60),
    DifferentialBaseLSN numeric(25,0),
    DifferentialBaseGUID uniqueidentifier,
    BackupTypeDescription nvarchar(60),
    BackupSetGUID uniqueidentifier,
    CompressedBackupSize bit,'
    + CASE WHEN (@SQLMajorVersion >= 11) THEN ' Containment tinyint,' ELSE '' END
	+ CASE WHEN (@SQLMajorVersion >= 12) THEN
		'KeyAlgorithm nvarchar(32),
		EncryptorThumbprint varbinary(20),
		EncryptorType nvarchar(32),' ELSE '' END
	+ '--
    -- This field added to retain order by
    --
    Seq int NOT NULL identity(1,1)
)

INSERT INTO @BackupSets
exec (''
RESTORE HEADERONLY
FROM ' + REPLACE(@bkup_files, '''', '''''') + '
WITH NOUNLOAD'')

SELECT * FROM @BackupSets
ORDER BY BackupFinishDate DESC
'

EXEC sp_executesql @HeaderOnly_cmd
