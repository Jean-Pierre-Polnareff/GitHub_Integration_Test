CREATE TABLE [dbo].[letter_report_transtion_data] (
    [EMonth]          DATE            NULL,
    [CustomerId]      BIGINT          NULL,
    [ClientId]        VARCHAR (10)    NULL,
    [LetterType]      VARCHAR (25)    NULL,
    [TypeFlg]         VARCHAR (5)     NOT NULL,
    [ReqDate]         DATE            NULL,
    [ListDate]        DATE            NULL,
    [StatusCode]      VARCHAR (25)    NULL,
    [CancelCode]      VARCHAR (25)    NULL,
    [LocationWorked]  VARCHAR (50)    NULL,
    [InitialBalance]  DECIMAL (19, 2) NULL,
    [LastPaymentDate] DATE            NULL,
    [CurrentBalance]  DECIMAL (19, 2) NULL,
    [SourceSystem2]   VARCHAR (50)    NULL,
    [WKNUMB]          VARCHAR (6)     NOT NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[letter_report_transtion_data] TO [CORP\aramugade]
    AS [dbo];

