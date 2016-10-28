DECLARE @TargetFolder NVARCHAR(255)

SET @TargetFolder = '$(targetfolder)'

SELECT DISTINCT
    SUBSTRING(volume_mount_point, 1, 1) AS volume_mount_point
    ,total_bytes/1024/1024 AS Total_MB
    ,available_bytes/1024/1024 AS Available_MB
	  ,ROUND(CONVERT(FLOAT, available_bytes)/CONVERT(FLOAT, total_bytes) * 100, 2) AS Percent_Free
FROM
    sys.master_files AS f
CROSS APPLY
    sys.dm_os_volume_stats(f.database_id, f.file_id)
WHERE SUBSTRING(volume_mount_point, 1, 1) = SUBSTRING(@TargetFolder, 1, 1)
