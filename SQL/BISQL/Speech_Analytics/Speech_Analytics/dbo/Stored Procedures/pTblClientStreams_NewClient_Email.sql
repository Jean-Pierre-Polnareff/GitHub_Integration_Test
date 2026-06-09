USE [DW_STAGING]
GO

/****** Object:  StoredProcedure [dbo].[pTblClientStreams_NewClient_Email]    Script Date: 4/16/2026 8:19:40 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[pTblClientStreams_NewClient_Email]
AS
SET NOCOUNT ON;

DROP TABLE IF EXISTS ##_New_Client__IDs_in_TCS_;
DROP TABLE IF EXISTS ##NewClientIDsinTCS;

SELECT L.Client_ID ["New Client ID"],Count(*) [        "# of LU_CUSTOMER acnts"] INTO ##_New_Client__IDs_in_TCS_ 
FROM DW_MSTR_DM.dbo.LU_Customer L (NoLock) LEFT JOIN DW_MSTR_DM.dbo.TblClientStreams T (NoLock)
ON L.Client_ID = T.Client_ID 
WHERE T.Client_ID IS NULL
GROUP BY L.Client_ID;

IF @@ROWCOUNT > 0 BEGIN
  INSERT DW_MSTR_DM.dbo.TblClientStreams(Client_ID,Insert_Date) SELECT ["New Client ID"],GetDate() FROM ##_New_Client__IDs_in_TCS_;

  EXEC msdb.dbo.sp_send_dbmail @profile_name='DW Mail',@recipients='Amod.Ramugade@radiusgs.com;sankeerth.mamidi@radiusgs.com',@copy_recipients='dw@radiusgs.com',@importance='High',
  @subject = 'New FACS Client IDs in [TblClientStreams] table',
  @query = 'SET NOCOUNT ON; SELECT * FROM ##_New_Client__IDs_in_TCS_',
  @query_result_header = 1;
END
ELSE BEGIN
  SELECT Client_ID ["New Client ID"] INTO ##NewClientIDsinTCS 
  FROM DW_MSTR_DM.dbo.TblClientStreams (NoLock)
  WHERE Parent IS NULL;
   
  IF @@ROWCOUNT > 0 BEGIN
    EXEC msdb.dbo.sp_send_dbmail @profile_name='DW Mail',@recipients='Amod.Ramugade@radiusgs.com;sankeerth.mamidi@radiusgs.com',@copy_recipients='dw@radiusgs.com',@importance='High',
    @subject = 'New FACS Client IDs in [TblClientStreams] table',
    @query = 'SET NOCOUNT ON; SELECT * FROM ##NewClientIDsinTCS',
    @query_result_header = 1;
  END;
END; 
GO

