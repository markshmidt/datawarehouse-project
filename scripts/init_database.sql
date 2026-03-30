USE master;
GO

-- Drop and recreate the 'DatawareHouse' database
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'DatawareHouse'
)
BEGIN
    ALTER DATABASE DatawareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DatawareHouse;
END;
GO

-- Create the 'DatawareHouse' database
CREATE DATABASE DatawareHouse;
GO

USE DatawareHouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO 
