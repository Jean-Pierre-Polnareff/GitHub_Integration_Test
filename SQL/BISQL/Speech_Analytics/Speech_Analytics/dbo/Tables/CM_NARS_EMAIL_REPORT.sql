CREATE TABLE [dbo].[CM_NARS_EMAIL_REPORT] (
    [Session_ID]          NVARCHAR (150) NULL,
    [Thread_ID]           BIGINT         NULL,
    [Started_Time]        TIME (7)       NULL,
    [Agent_Logon_Id]      NVARCHAR (50)  NULL,
    [Sent_Date]           DATE           NULL,
    [TFH_Result]          NVARCHAR (150) NULL,
    [Account_Key]         NVARCHAR (150) NULL,
    [Email_Address]       NVARCHAR (200) NULL,
    [Interaction_Type]    NVARCHAR (50)  NULL,
    [Total_Emails]        INT            NULL,
    [LiveVox_Client_Name] NVARCHAR (50)  NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NARS_EMAIL_REPORT] TO [CORP\aramugade]
    AS [dbo];

