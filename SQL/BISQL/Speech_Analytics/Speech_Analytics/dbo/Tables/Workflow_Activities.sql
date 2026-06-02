CREATE TABLE [dbo].[Workflow_Activities] (
    [id]            SMALLINT     IDENTITY (1, 1) NOT NULL,
    [activity_name] VARCHAR (50) NULL,
    [sort]          SMALLINT     DEFAULT ((0)) NOT NULL,
    [IsActive]      BIT          DEFAULT ((1)) NOT NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[Workflow_Activities] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[Workflow_Activities] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[Workflow_Activities] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[Workflow_Activities] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT INSERT
    ON OBJECT::[dbo].[Workflow_Activities] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[Workflow_Activities] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[Workflow_Activities] TO [CORP\dmukherji]
    AS [dbo];

