CREATE TABLE [dbo].[CM_Comment_API] (
    [ClientCaptureDate]   DATETIME2 (7)  NULL,
    [CallID]              BIGINT         NOT NULL,
    [AgentId]             NVARCHAR (150) NULL,
    [ReportType]          NVARCHAR (150) NOT NULL,
    [UserName]            NVARCHAR (200) NULL,
    [ObservationType]     NVARCHAR (50)  NULL,
    [ObservationComments] NVARCHAR (MAX) NULL,
    [LastModified]        DATETIME2 (7)  NULL,
    [CommentID]           NVARCHAR (150) NULL,
    [Insert_Date]         DATETIME2 (7)  NOT NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Comment_API] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Comment_API] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Comment_API] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Comment_API] TO [CORP\pjain]
    AS [dbo];

