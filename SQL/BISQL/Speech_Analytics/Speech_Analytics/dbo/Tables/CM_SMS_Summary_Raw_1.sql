CREATE TABLE [dbo].[CM_SMS_Summary_Raw] (
    [DateStarted]    DATETIME        NOT NULL,
    [ServiceName]    NVARCHAR (MAX)  NOT NULL,
    [ServiceID]      NVARCHAR (MAX)  NOT NULL,
    [FileName]       NVARCHAR (MAX)  NOT NULL,
    [SMSTotalCalls]  BIGINT          NOT NULL,
    [AccountsWorked] BIGINT          NOT NULL,
    [SMSMTDelivered] BIGINT          NOT NULL,
    [TotalCalls]     BIGINT          NOT NULL,
    [RPCs]           BIGINT          NOT NULL,
    [Payments]       BIGINT          NULL,
    [PaymentAmt]     DECIMAL (19, 2) NULL,
    [SMSStopText]    BIGINT          NOT NULL,
    [SMSConnects]    BIGINT          NOT NULL,
    [Insert_Date]    DATETIME        NOT NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SMS_Summary_Raw] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_Summary_Raw] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SMS_Summary_Raw] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_Summary_Raw] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SMS_Summary_Raw] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SMS_Summary_Raw] TO [CORP\pjain]
    AS [dbo];


GO
GRANT INSERT
    ON OBJECT::[dbo].[CM_SMS_Summary_Raw] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SMS_Summary_Raw] TO [CORP\pjain]
    AS [dbo];

