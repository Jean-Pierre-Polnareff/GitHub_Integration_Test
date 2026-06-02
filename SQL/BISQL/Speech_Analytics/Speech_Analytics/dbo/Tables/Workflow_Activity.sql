CREATE TABLE [dbo].[Workflow_Activity] (
    [activity_id]   BIGINT           IDENTITY (1, 1) NOT NULL,
    [activity_name] VARCHAR (50)     NULL,
    [session_id]    UNIQUEIDENTIFIER NULL,
    [error_msg]     VARCHAR (2000)   NULL,
    [IsProcessed]   BIT              DEFAULT ((0)) NOT NULL,
    [start_time]    DATETIME         DEFAULT (getdate()) NOT NULL,
    [end_time]      DATETIME         NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[Workflow_Activity] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[Workflow_Activity] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[Workflow_Activity] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[Workflow_Activity] TO [CORP\dmukherji]
    AS [dbo];

