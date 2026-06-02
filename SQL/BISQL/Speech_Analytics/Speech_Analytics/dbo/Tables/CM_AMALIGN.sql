CREATE TABLE [dbo].[CM_AMALIGN] (
    [EOM]             DATE           NOT NULL,
    [Agent_ID]        NVARCHAR (150) NOT NULL,
    [Collector]       NVARCHAR (150) NULL,
    [Supervisor]      NVARCHAR (150) NULL,
    [Manager]         NVARCHAR (150) NULL,
    [Portfolio]       NVARCHAR (200) NULL,
    [Date_of_Joining] DATE           NULL
);




GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\pjain]
    AS [dbo];


GO
GRANT INSERT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AMALIGN] TO [CORP\aramugade]
    AS [dbo];

