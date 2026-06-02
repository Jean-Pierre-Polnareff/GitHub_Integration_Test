CREATE TABLE [dbo].[CM_RPT_Email_Lxa] (
    [BOMDate]                DATE           NULL,
    [Session_ID]             NVARCHAR (150) NULL,
    [Thread_ID]              BIGINT         NULL,
    [Started_Time]           TIME (7)       NULL,
    [Agent_Logon_Id]         NVARCHAR (50)  NULL,
    [Sent_Date]              DATE           NULL,
    [TFH_Result]             NVARCHAR (150) NULL,
    [Account_Key]            NVARCHAR (150) NULL,
    [Email_Address]          NVARCHAR (200) NULL,
    [Interaction_Type]       NVARCHAR (50)  NULL,
    [Next_TFH_Result]        NVARCHAR (150) NULL,
    [Next_Ststime_Result]    TIME (7)       NULL,
    [Next_Session_id_Result] NVARCHAR (150) NULL,
    [Next_Sent_Date_Result]  DATE           NULL,
    [Res_Re_Checker]         VARCHAR (3)    NOT NULL,
    [combinedDateTime]       DATETIME       NULL,
    [Next_combinedDateTime]  DATETIME       NULL,
    [diffHours]              INT            NULL,
    [Next_Reply_Status]      INT            NOT NULL,
    [EMAIL_Count]            INT            NOT NULL,
    [POPMET]                 INT            NOT NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_Email_Lxa] TO [CORP\aramugade]
    AS [dbo];

