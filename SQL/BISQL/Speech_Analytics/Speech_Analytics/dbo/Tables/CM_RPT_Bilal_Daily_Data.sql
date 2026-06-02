CREATE TABLE [dbo].[CM_RPT_Bilal_Daily_Data] (
    [KeyCustomer]         BIGINT          NOT NULL,
    [KeySourceSystem]     INT             NULL,
    [CustomerId]          BIGINT          NULL,
    [ClientId]            VARCHAR (10)    NULL,
    [SourceSystem]        VARCHAR (50)    NULL,
    [ListDate]            DATE            NULL,
    [StatusCode]          VARCHAR (25)    NULL,
    [InitialBalance]      DECIMAL (19, 2) NULL,
    [CustomerState]       VARCHAR (10)    NULL,
    [OfficeID]            VARCHAR (50)    NULL,
    [InitialBalanceRange] VARCHAR (50)    NULL,
    [ClientStreamId]      VARCHAR (50)    NULL,
    [ScoreGroup]          VARCHAR (25)    NULL,
    [LetterType]          VARCHAR (50)    NULL,
    [SENDTYPE]            VARCHAR (5)     NULL,
    [CalendarDate]        DATE            NULL,
    [Score]               FLOAT (53)      NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Bilal_Daily_Data] TO [CORP\aramugade]
    AS [dbo];

