/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
File: status.sql
Description : Data status codes in the ingestion pipeline
Edit History:
  Ver   Date         Author     Description
  0.1   5/16/20      SHONG      Initial version
  0.2	8/30/20      SHONG	    Update for v1
*************************************************************/
CREATE TABLE CDMH_STAGING.STATUS 
(
  STATUS VARCHAR2(255 BYTE) 
, STATUS_ID NUMBER(*, 0) NOT NULL 
) 
;

Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Load to Instance in Progress',20);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Load to Instance Complete',21);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Instance DQD Report in Progress',22);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Instance DQD Report Complete',23);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Load to DataStore in Progress',27);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Load to DataStore Complete',28);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Ready For Instance',17);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Skip Processing',101);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Generating Person Clean',9);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Generated Person Clean',10);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Generating N3CDS_Domain_Map_Id',11);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Generated N3CDS_Domain_Map_Id',12);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Instance Cleanup in Progress',18);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Instance Cleanup Complete',19);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('DataStore Cleanup in Progress',25);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('DataStore Cleanup Complete',26);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Staging Cleanup in Progress',13);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Staging Cleanup Complete',14);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Load to Staging in Progress',15);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Load to Staging Complete',16);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Ready For DataStore',24);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Error Processing',0);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('DataSet Received',1);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Load to Native in Progress',5);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Load to Native Complete',6);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Ready For Native',2);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Native Correction in Progress',7);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Native Correction Complete',8);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Queued',100);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Native Cleanup In Progress',3);
Insert into CDMH_STAGING.STATUS (STATUS,STATUS_ID) values ('Native Cleanup Complete',4);
