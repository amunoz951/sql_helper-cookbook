SET NOCOUNT ON

--:SETVAR jobname "Test Job Name"
--:SETVAR jobowner "domain\user"
--:SETVAR jobowner "sa"

DECLARE @delete_result int
DECLARE @jobId binary(16)

SELECT @jobId = job_id FROM msdb.dbo.sysjobs WHERE (name = N'$(jobname)')
IF (@jobId IS NOT NULL)
BEGIN
    EXEC  @delete_result = msdb.dbo.sp_delete_job @jobId

	IF @delete_result = 1
	BEGIN
		PRINT ('Failed to delete job. Aborting operation.')
		SET NOEXEC ON
	END
END
GO

EXEC  msdb.dbo.sp_add_job @job_name=N'$(jobname)',
		@enabled=1,
		@notify_level_eventlog=0,
		@notify_level_email=2,
		@notify_level_netsend=2,
		@notify_level_page=2,
		@delete_level=1,
		@category_name=N'[Uncategorized (Local)]',
		@owner_login_name=N'$(jobowner)'

GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'$(jobname)'
GO
USE [msdb]
GO

DECLARE @jobstep_cmd NVARCHAR(max)
DECLARE @current_stepname NVARCHAR(max)

SET @jobstep_cmd = '
	$(sqlquery)
	'

SET @current_stepname = 'Run TSQL'

DECLARE @onsuccessaction int = 1

EXEC msdb.dbo.sp_add_jobstep @job_name=N'$(jobname)', @step_name=@current_stepname,
		@cmdexec_success_code=0,
		@on_success_action=@onsuccessaction,
		@on_fail_action=2,
		@retry_attempts=0,
		@retry_interval=0,
		@os_run_priority=0, @subsystem=N'TSQL',
		@command=@jobstep_cmd,
		@database_name=N'master',
		@flags=12

--EXEC msdb.dbo.sp_add_jobschedule @job_name=N'$(jobname)', @name=N'Restart Job',
--		@enabled=1,
--		@freq_type=4,
--		@freq_interval=1,
--		@freq_subday_type=8,
--		@freq_subday_interval=1,
--		@freq_relative_interval=0,
--		@freq_recurrence_factor=1,
--		@active_start_date=20160127,
--		@active_end_date=99991231,
--		@active_start_time=1000,
--		@active_end_time=235959

IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE name LIKE '$(jobname)')
BEGIN
	PRINT ('Job ''$(jobname)'' created on ' + CONVERT(nvarchar, @@SERVERNAME) + '.')
END
ELSE
BEGIN
	PRINT ('Job ''$(jobname)'' could not be created on ' + CONVERT(nvarchar, @@SERVERNAME) + '.')
END
GO

EXEC msdb.dbo.sp_start_job @job_name=N'$(jobname)'
GO
