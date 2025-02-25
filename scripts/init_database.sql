/*
=============================================================
Create Database and Schemas
=============================================================
Script purpose: 
  This script creates a new database named 'DataWarehouse' after checking if it already exsits.
  Additionally, the scrip sets up three schemas within database: 'bronze', 'silver', 'gold'.
*/

--Create Database 'DataWarehouse'
CREATE DATABASE IF NOT EXISTS DataWarehouse;

USE DataWarehouse;

--Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
