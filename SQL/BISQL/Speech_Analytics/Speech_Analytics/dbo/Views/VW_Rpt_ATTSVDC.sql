/****** Script for SelectTopNRows command from SSMS  ******/
create view VW_Rpt_ATTSVDC
as
 select [EOM]
      ,[AGNTID]
      ,[CALLID]
      ,[CALLDT]
      ,[RGSSEID]
      ,[DIS1]
      ,[RECDP]
      ,[PHNUMB]
      ,[CLNTID]
      ,[SILT]
      ,[CLDUR]
      ,[CATCNT]
      ,[DIS1CAT]
      ,[OPNCAL]
      ,[CALLCOUNT]
      ,[SCNDVC]
      ,[SCNDVCTM]
      ,[PVER]
      ,[PVERTM]
      ,[CXCATHITAGNT]
      ,[CXCOMPNAMEAGNT]
      ,[CXLANGTIMEAGNT]
      ,[CXCATHITSUP]
      ,[CXCOMPNAMESUP]
      ,[CXLANGTIMESUP]
      ,[CXLANGAGNTGRP]
      ,[CXLANGSUPGRP]
      ,[ACTTALKTIME]
      ,[SUPTIME]
      ,[AGNTTIME]
      ,[PTP_Flags]
      ,[SCNDVC_Flags]
      ,[BEHA_Change_Flag]
	from Speech_Analytics.dbo.[CM_Rpt_ATTSVDC]
 -- FROM [CLIENT_ANALYTICS].[dbo].[CM_Rpt_ATTSVDC]
--VW_Rpt_ATTSVDC
  --sp_spaceused 'CM_Rpt_ATTSVDC'
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_ATTSVDC] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_ATTSVDC] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_ATTSVDC] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_ATTSVDC] TO [CORP\mhuang]
    AS [dbo];

