DECLARE @Depth int
DECLARE @FileFlag int
DECLARE @TargetFolder nvarchar(255)
DECLARE @BackupBaseName nvarchar(255)

SET @Depth = 1
SET @FileFlag = 1
SET @TargetFolder = '$(targetfolder)'
SET @BackupBaseName = '$(bkupname)'

DECLARE @DirTree TABLE (
    FileName nvarchar(255),
    Depth smallint,
    FileFlag smallint
   )

INSERT INTO @DirTree
EXEC xp_dirtree @TargetFolder, @Depth, @FileFlag

IF EXISTS (SELECT * FROM @DirTree WHERE FileFlag = @FileFlag AND LOWER([FileName]) LIKE LOWER(@BackupBaseName + '.bak'))
BEGIN
  SELECT * FROM @DirTree
  WHERE FileFlag = @FileFlag AND LOWER([FileName]) LIKE LOWER(@BackupBaseName + '.bak')
END
ELSE IF EXISTS (SELECT * FROM @DirTree WHERE FileFlag = @FileFlag AND LOWER([FileName]) LIKE LOWER(@BackupBaseName + 'part%.bak'))
BEGIN
  SELECT * FROM @DirTree
  WHERE FileFlag = @FileFlag AND LOWER([FileName]) LIKE LOWER(@BackupBaseName + 'part%.bak')
END
ELSE
BEGIN
  SELECT * FROM @DirTree
  WHERE FileFlag = @FileFlag AND LOWER([FileName]) LIKE LOWER(@BackupBaseName + '%.bak')
END
