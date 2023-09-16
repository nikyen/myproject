--Name : Hive HQL
--Owner : Sayu Softtech
--Date cre : 08102022
--Modified date : 
--For more info follow us on instagram


create database prod;

use prod;

create table subscriber_details_staging (subscriberId integer,subscribername string,contactnumber integer,emailId string,address string,city string,country string,create_date timestamp,update_date timestamp,Active_flag char(1),prepaid_desc string,pre_amount integer,postpaid_desc string,pos_amount integer)
Row format delimited
fields terminated by ',';

create table subscriber_details (subscriberId integer,subscribername string,contactnumber integer,emailId string,address string,city string,country string,create_date timestamp,update_date timestamp,Active_flag char(1),prepaid_desc string,pre_amount integer,postpaid_desc string,pos_amount integer)
Row format delimited
fields terminated by ',';

create table complaint_details_staging (subscriberId integer,subscribername string,complaintId integer,complaintReg string,description string,com_cre_date timestamp,com_upd_date timestamp,status string)
Row Format delimited
fields terminated by ',';

create table complaint_details (subscriberId integer,subscribername string,complaintId integer,complaintReg string,description string,com_cre_date timestamp,com_upd_date timestamp,status string)
Row Format delimited
fields terminated by ',';

--completed---





