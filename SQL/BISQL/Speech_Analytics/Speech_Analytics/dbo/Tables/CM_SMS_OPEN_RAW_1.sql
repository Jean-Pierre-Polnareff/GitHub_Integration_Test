CREATE TABLE [dbo].[CM_SMS_OPEN_RAW] (
    [LetterCode]       NVARCHAR (150) NULL,
    [LetterID]         NVARCHAR (MAX) NULL,
    [Customer]         NVARCHAR (150) NULL,
    [SMSRequestedTime] DATETIME2 (7)  NULL,
    [SMSRequestedDate] DATE           NULL,
    [SMSClickedTime]   DATETIME2 (7)  NULL,
    [SMSClickedDate]   DATE           NULL,
    [AccountNumber]    NVARCHAR (150) NULL,
    [FileName]         NVARCHAR (150) NULL,
    [ClientName]       NVARCHAR (MAX) NULL,
    [ClientID]         NVARCHAR (150) NULL,
    [Insert_Date]      DATE           NULL,
    [EndofMonthReq]    DATE           NULL,
    [EndofMonthOpen]   DATE           NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [CORP\smamidi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_OPEN_RAW] TO [CORP\pjain]
    AS [dbo];

