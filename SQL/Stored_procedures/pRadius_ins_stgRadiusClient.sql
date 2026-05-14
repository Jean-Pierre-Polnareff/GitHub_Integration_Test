USE [DW_STAGING]
GO

/****** Object:  StoredProcedure [dbo].[pRadius_ins_stgRadiusClient]    Script Date: 4/16/2026 9:12:33 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[pRadius_ins_stgRadiusClient] (@avFeedName VARCHAR(200),@aiKeyETLAuditHistory INT,@aiKeySourceSystem INT)
AS
SET NOCOUNT ON;

DECLARE 
@vInsert	VARCHAR(1000),
@vSubject	VARCHAR(500) = @@SERVERNAME + '.' + DB_NAME() + '.dbo.' + OBJECT_NAME(@@PROCID),
@vError		VARCHAR(8000);

BEGIN TRY
  TRUNCATE TABLE DW_STAGING.dbo.dfRadiusClient;
  TRUNCATE TABLE DW_STAGING.dbo.dfRadiusClientAMEX;
  TRUNCATE TABLE DW_STAGING.dbo.dfRadiusClientARTIVA;

  IF @aiKeySourceSystem != 3 BEGIN
	SET @vInsert = 'SET NOCOUNT ON; BULK INSERT DW_STAGING.dbo.dfRadiusClientARTIVA FROM ''\\Dfw2-bisql-001\SSISFlatFileStage\Client\Queue\' + @avFeedName + ''' WITH (FORMAT=''CSV'',FIRSTROW=2,CODEPAGE=''RAW'',MAXERRORS=0,TABLOCK)';
  END
  ELSE IF @aiKeySourceSystem = 3 BEGIN
	SET @vInsert = 'SET NOCOUNT ON; BULK INSERT DW_STAGING.dbo.dfRadiusClientAMEX FROM ''\\Dfw2-bisql-001\SSISFlatFileStage\Client\Queue\' + @avFeedName + ''' WITH (FORMAT=''CSV'',FIRSTROW=2,CODEPAGE=''RAW'',MAXERRORS=0,TABLOCK)';
  END;
  
  EXEC (@vInsert);

  UPDATE H SET H.NumRaw = R.NumRaw, H.UpdateDate = GETDATE()
  FROM DW_MSTR_DM.dbo.ETLauditHistory H JOIN 
  (SELECT @aiKeyETLAuditHistory KeyETLAuditHistory,(SELECT Count(*) FROM DW_STAGING.dbo.dfRadiusClientAMEX) + (SELECT Count(*) FROM DW_STAGING.dbo.dfRadiusClientARTIVA) NumRaw) R
  ON H.KeyETLAuditHistory = R.KeyETLAuditHistory;

  INSERT DW_STAGING.dbo.dfRadiusClient(
         [SourceSystem],[ClientId],[ClientParent],[ClientStreamId],[ClientStream],[Age],[PaperType],[PaperTypeGrouping],[SIFPercent],[Commission],[RecallPeriod],[LocationWorked],[ClientClass],[ClientClassDescription],[LastModifiedDate],            [DataSourceFileName],[ClientSegmentationGroup],                      [KeyETLAuditHistory],IsDebtBuyer)
  SELECT [SourceSystem],[ClientId],[ClientParent],[ClientStreamId],[ClientStream],[Age],[PaperType],[PaperTypeGrouping],[SIFPercent],[Commission],[RecallPeriod],[LocationWorked],[ClientClass],[ClientClassDescription],[LastModifiedDate],@avFeedName [DataSourceFileName],[ClientSegmentationGroup],@aiKeyETLAuditHistory [KeyETLAuditHistory],iSDebtBuyer 
  FROM [DW_STAGING].[dbo].[vwRadiusClient];

  DROP TABLE IF EXISTS #tETLkey;
  CREATE TABLE #tETLkey(KeyETLAuditHistory BIGINT);

  DROP TABLE IF EXISTS #stgClient;

  SELECT RowNumber = ROW_NUMBER() OVER(PARTITION BY ISNULL(CAST(ClientId  AS VARCHAR(10)),''), ISNULL(CAST(SourceSystem AS VARCHAR(50)),'') ORDER BY DataSourceFileName DESC) 
	,ClientId = ISNULL(CAST(ClientId  AS VARCHAR(10)),'')
	,SourceSystem = ISNULL(CAST(SourceSystem AS VARCHAR(50)),'')
	,ClientParent = ISNULL(CAST(ClientParent AS VARCHAR(50)),'')
	,ClientStreamId = ISNULL(CAST(ClientStreamId AS VARCHAR(50)),'')
	,ClientStream = ISNULL(CAST(ClientStream AS VARCHAR(50)),'')
	,Age = CAST(AGE AS DATE) 
	,PaperType = ISNULL(CAST(PaperType AS VARCHAR(50)),'')
	,PaperTypeGrouping = ISNULL(CAST(PaperTypeGrouping AS VARCHAR(50)),'')
	,SIFPercent = CAST(ISNULL(NULLIF(SIFPercent,''),'0') AS DECIMAL(19,2))
	,Commission = ISNULL(CAST(Commission AS VARCHAR(50)),'')
	,RecallPeriod = ISNULL(CAST(RecallPeriod AS VARCHAR(50)),'')
	,LocationWorked = ISNULL(CAST(LocationWorked AS VARCHAR(50)),'')
	,ClientClass = ISNULL(CAST(ClientClass AS VARCHAR(50)),'')
	,ClientClassDescription = ISNULL(CAST(ClientClassDescription AS VARCHAR(50)),'')
	,ClientSegmentationGroup = ISNULL(CAST(ClientSegmentationGroup AS VARCHAR(50)),'')
	,DataSourceFileName
	,KeyETLAuditHistory
	,iSDebtBuyer
  INTO #stgClient
  FROM DW_Staging.dbo.dfRadiusClient;

  TRUNCATE TABLE DW_Staging.dbo.stgRadiusClient;

  INSERT INTO DW_Staging.dbo.stgRadiusClient(
         ClientId,SourceSystem,ClientParent,ClientStreamId,ClientStream,Age,PaperType,PaperTypeGrouping,SIFPercent,Commission,RecallPeriod,LocationWorked,ClientClass,ClientClassDescription,DataSourceFileName,ClientSegmentationGroup,KeyETLAuditHistory,IsDebtBuyer) OUTPUT INSERTED.KeyETLAuditHistory INTO #tETLkey
  SELECT ClientId,SourceSystem,ClientParent,ClientStreamId,ClientStream,Age,PaperType,PaperTypeGrouping,SIFPercent,Commission,RecallPeriod,LocationWorked,ClientClass,ClientClassDescription,DataSourceFileName,ClientSegmentationGroup,KeyETLAuditHistory,IsDebtBuyer  
  FROM #stgClient WHERE RowNumber = 1;		

  UPDATE H SET H.NumStg = S.NumStg 
  FROM DW_MSTR_DM.dbo.ETLauditHistory H JOIN 
  (SELECT KeyETLAuditHistory, Count(*) NumStg FROM #tETLkey GROUP BY KeyETLAuditHistory) S
  ON H.KeyETLAuditHistory = S.KeyETLAuditHistory;

  RETURN 0;
END TRY
BEGIN CATCH
  SET @vError = IsNull(ERROR_MESSAGE(),'');
  UPDATE DW_MSTR_DM.dbo.ETLauditHistory SET ErrMsg = @vError, UpdateDate = GetDate() WHERE KeyETLAuditHistory = @aiKeyETLAuditHistory;
  SET @vError = 'SELECT * FROM DW_MSTR_DM.dbo.ETLauditHistory (NoLock) WHERE KeyETLAuditHistory = ' + Cast(@aiKeyETLAuditHistory AS VARCHAR) + CHAR(13) + CHAR(13) + @vError;
  EXEC msdb.dbo.sp_send_dbmail @profile_name = 'DW Mail', @recipients = 'dw@radiusgs.com', @importance = 'High', @subject = @vSubject, @body = @vError;

  RETURN 1;
END CATCH;
GO

