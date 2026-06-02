CREATE TABLE [dbo].[CM_RadiusCall_LV_Miss] (
    [Call_Date]               DATETIME       NULL,
    [Tenant_ID]               INT            NULL,
    [Client_ID]               NVARCHAR (50)  NULL,
    [Call_Center_Name]        NVARCHAR (150) NULL,
    [Call_Center_Id]          INT            NULL,
    [LV_Client_Name]          NVARCHAR (150) NULL,
    [Service_Name]            NVARCHAR (150) NULL,
    [Service_Type]            NVARCHAR (150) NULL,
    [Service_Id]              INT            NULL,
    [Transaction_Type]        NVARCHAR (150) NULL,
    [Answer_Type]             NVARCHAR (150) NULL,
    [Session_Id]              NVARCHAR (150) NOT NULL,
    [Transaction_ID]          FLOAT (53)     NULL,
    [Phone_Dialed]            FLOAT (53)     NULL,
    [Original_Account_Number] NVARCHAR (150) NULL,
    [Client_Name]             NVARCHAR (150) NULL,
    [First_Name]              NVARCHAR (150) NULL,
    [Last_Name]               NVARCHAR (150) NULL,
    [Call_Connect_Time_CT]    TIME (6)       NOT NULL,
    [Call_End_Time]           TIME (6)       NOT NULL,
    [Call_Duration]           INT            NOT NULL,
    [IVR_Duration]            INT            NOT NULL,
    [Hold_Time]               INT            NULL,
    [Transfer_Duration]       INT            NULL,
    [File_Name]               NVARCHAR (150) NULL,
    [Agent_Logon_Id]          NVARCHAR (150) NULL,
    [Agent_Full_Name]         NVARCHAR (200) NULL,
    [Agent_Team]              NVARCHAR (150) NULL,
    [Talk_Time]               INT            NULL,
    [Wrap_Time]               INT            NULL,
    [Agent_Hold_Time]         INT            NULL,
    [Livevox_Result]          NVARCHAR (150) NULL,
    [Result_Code]             INT            NULL,
    [Result_Id]               INT            NULL,
    [Agent_Desktop_Outcome]   NVARCHAR (150) NULL,
    [Result_Category]         NVARCHAR (150) NULL,
    [Custom_Outcome_1]        NVARCHAR (150) NULL,
    [Custom_Outcome_2]        NVARCHAR (150) NULL,
    [Custom_Outcome_3]        NVARCHAR (150) NULL,
    [Zip]                     NVARCHAR (50)  NULL,
    [Extra_4]                 NVARCHAR (100) NULL,
    [Extra_5]                 NVARCHAR (100) NULL,
    [Extra_6]                 NVARCHAR (100) NULL,
    [Extra_9]                 NVARCHAR (100) NULL,
    [Extra_12]                NVARCHAR (100) NULL,
    [Extra_16]                NVARCHAR (100) NULL,
    [Extra_17]                NVARCHAR (50)  NULL,
    [Extra_20]                NVARCHAR (100) NULL,
    [Future_Use_1]            NVARCHAR (100) NULL,
    [Future_Use_2]            NVARCHAR (100) NULL,
    [caller_id]               FLOAT (53)     NULL,
    [phone_number]            FLOAT (53)     NULL,
    [Question_1]              NVARCHAR (100) NULL,
    [Question_2]              NVARCHAR (100) NULL,
    [Question_3]              NVARCHAR (100) NULL,
    [Question_4]              NVARCHAR (100) NULL,
    [Question_5]              NVARCHAR (100) NULL,
    [Question_6]              NVARCHAR (100) NULL,
    [Question_7]              NVARCHAR (100) NULL,
    [Question_8]              NVARCHAR (100) NULL,
    [Question_9]              NVARCHAR (100) NULL,
    [Question_10]             NVARCHAR (100) NULL,
    [Is_RPC]                  INT            NULL,
    [Is_Promise]              INT            NULL,
    [Call_Year]               INT            NULL,
    [Consumer_ID]             NVARCHAR (100) NULL,
    [Is_Connect]              INT            NULL,
    [Total_Calls]             INT            NULL,
    [Insert_Date]             DATETIME       NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RadiusCall_LV_Miss] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RadiusCall_LV_Miss] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RadiusCall_LV_Miss] TO [CORP\dmukherji]
    AS [dbo];

