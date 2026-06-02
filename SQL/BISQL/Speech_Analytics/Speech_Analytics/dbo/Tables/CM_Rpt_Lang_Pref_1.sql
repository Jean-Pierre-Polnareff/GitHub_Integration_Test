CREATE TABLE [dbo].[CM_Rpt_Lang_Pref] (
    [EOM]                  DATE            NULL,
    [AGNTID]               NVARCHAR (100)  NULL,
    [CALLID]               FLOAT (53)      NULL,
    [CALLDT]               DATE            NULL,
    [RECDP]                NVARCHAR (15)   NULL,
    [DIS1]                 NVARCHAR (100)  NULL,
    [SILT]                 FLOAT (53)      NULL,
    [CLDUR]                FLOAT (53)      NULL,
    [DIR]                  NVARCHAR (50)   NULL,
    [CLNTID]               NVARCHAR (200)  NULL,
    [placedate]            DATE            NULL,
    [CrmStat]              VARCHAR (5)     NULL,
    [CurrentBalance]       MONEY           NULL,
    [CXCity]               VARCHAR (30)    NULL,
    [CxState]              VARCHAR (3)     NULL,
    [DIS1CAT]              NVARCHAR (10)   NULL,
    [CRMLang]              VARCHAR (30)    NULL,
    [SALang]               VARCHAR (9)     NULL,
    [DateUpdated]          DATE            NULL,
    [SaLangTm]             FLOAT (53)      NULL,
    [LVCustomerID]         VARCHAR (255)   NULL,
    [CustomerID]           VARCHAR (255)   NULL,
    [RGSSEID]              NVARCHAR (4000) NULL,
    [CallCount]            INT             NOT NULL,
    [Applicable]           INT             NOT NULL,
    [Possible Observation] INT             NOT NULL,
    [MissedCrm]            INT             NOT NULL,
    [RPCCRMINDICATOR]      NVARCHAR (50)   NULL,
    [accclosed]            DATE            NULL,
    [accreturn]            DATE            NULL,
    [contactdate]          DATE            NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\smamidi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\smamidi]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\smamidi]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\svcbisql]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\svcbisql]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\svcbisql]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\svc_veeam]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\svc_veeam]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_Lang_Pref] TO [CORP\svc_veeam]
    AS [dbo];

