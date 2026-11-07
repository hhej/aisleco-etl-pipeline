-- =====================================================================
-- Aisle & Co. — walkthrough on OLTP v4 lean (Problems 1 and 2)
-- Load after OLTP v4 + DW v6 DDL. Each PART ends with the ETL run the
-- business would do at that point (see the psql commands in the comments).
-- "-- @step <date>" comments mark the business day of the inquiry events in parts 1-3, so
-- the demo can also replay them one day at a time (scripts/load_walkthrough.py, steps()).
-- =====================================================================
\set ON_ERROR_STOP 1

-- =============== PART 0: reference data already in the CRM ===============
INSERT INTO master."MarketingChannel" VALUES
 (1,'LINE OA','Online',NULL,'Active'),(2,'Facebook','Online',NULL,'Active'),(3,'Instagram','Online',NULL,'Active'),
 (4,'TikTok','Online','#TIKTOK','Active'),(5,'Website','Online','#WEB','Active'),(6,'Wedding Fair','Offline','#FAIR','Active'),
 (7,'Referral','Offline',NULL,'Active'),(8,'Walk-in','Offline',NULL,'Active');
INSERT INTO master."Package" VALUES
 (1,'Full Planning','Full',NULL,'Percent',12.5,'2025-01-01','2026-06-30','Retired'),
 (2,'Full Planning','Full',NULL,'Percent',13.5,'2026-07-01',NULL,'Active'),
 (3,'Partial Planning','Partial',90000,'Fixed',NULL,'2025-01-01',NULL,'Active');
INSERT INTO master."EmployeeRole" VALUES (1,'Lead Planner','Active'),(2,'Planner','Active'),(3,'Coordinator','Active'),(4,'Event Staff','Active'),(5,'Sales Executive','Active');
INSERT INTO master."Employee" VALUES
 (1,1,'E001','Som','S.','FullTime','2022-01-10',50,25,'Active'),
 (2,2,'E002','Nina','N.','FullTime','2023-05-01',44,20,'Active'),
 (3,3,'E003','Beam','B.','FullTime','2024-02-01',44,30,'Active'),
 (4,4,'E004','Arm','A.','FullTime','2024-06-01',40,40,'Active'),
 (5,5,'E005','May','M.','FullTime','2023-01-01',44,NULL,'Active');
INSERT INTO master."AuspiciousDate"("CalendarDate","DateType","AuspiciousLevel") VALUES
 ('2027-01-16','ThaiAstrology','High'),('2027-02-13','ThaiAstrology','High'),('2027-02-14','ChineseCalendar','Medium'),('2027-02-20','ThaiAstrology','Medium');

-- Weddings already booked for the Valentine's weekend (13-14 Feb 2027), planned since October
INSERT INTO master."Customer"("CustomerID","Address","CreatedDate")
 SELECT 100+k, '1 Soi 1, Bang Na, Bangkok', '2026-10-01' FROM generate_series(1,4) k;
INSERT INTO transaction."Event"("EventID","CustomerID","PackageID","EventDate","GuestCount","ComplexityLevel","AgreedPackagePrice","Status","CreatedDate") VALUES
 (101,101,2,'2027-02-13',300,'Medium',110000,'Confirmed','2026-10-05'),(102,102,3,'2027-02-13',250,'Low',90000,'Confirmed','2026-10-08'),
 (103,103,2,'2027-02-14',480,'High',150000,'Confirmed','2026-10-12'),(104,104,3,'2027-02-14',220,'Low',90000,'Confirmed','2026-10-20');
INSERT INTO transaction."EventEmployeeAssignment"("EventID","EmployeeID","AssignmentRole","AssignedDate","WorkloadHours","IsLead","Status") VALUES
 (101,1,'Lead Planner','2026-10-06',70,'Y','Active'),(101,3,'Coordinator','2027-01-10',20,'N','Active'),(101,4,'Event Staff','2027-01-20',12,'N','Active'),
 (102,2,'Lead Planner','2026-10-09',30,'Y','Active'),(102,3,'Coordinator','2027-01-10',18,'N','Active'),(102,4,'Event Staff','2027-01-20',12,'N','Active'),
 (103,1,'Lead Planner','2026-10-13',90,'Y','Active'),(103,3,'Coordinator','2027-01-12',24,'N','Active'),(103,4,'Event Staff','2027-01-20',12,'N','Active'),
 (104,2,'Lead Planner','2026-10-21',28,'Y','Active'),(104,3,'Coordinator','2027-01-12',16,'N','Active'),(104,4,'Event Staff','2027-01-20',12,'N','Active');

-- =============== PART 1: Ploy & Ton (fair -> booked) ===============
-- @step 2026-03-14
-- Sat 14 Mar 2026 14:05: tablet form at the Wedding Fair booth
INSERT INTO master."Customer" VALUES (1,'-, Chatuchak, Bangkok','2026-03-14 14:05');
INSERT INTO master."CustomerContact"("CustomerID","ContactName","ContactTitle","Phone","Email","IsPrimaryContact") VALUES
 (1,'Ploy','Bride','081-xxx-1111','ploy@example.com','Y'),(1,'Ton','Groom','089-xxx-2222','ton@example.com','N');
INSERT INTO transaction."Inquiry"("InquiryID","CustomerID","ChannelID","IndicativePackageID","InquiryDate","RequestedEventDate","BudgetRange","ContactedDate","Status")
 VALUES (1,1,6,1,'2026-03-14 14:05','2027-01-16','500k-1M','2026-03-14 14:05','Contacted');
-- 14:20 she scans the booth QR: LINE opens with "#FAIR" typed in. Webhook -> account + message.
INSERT INTO master."CustomerChannelAccount" VALUES (1,1,'LINE','U4af4980629aa','Ploy','2026-03-14 14:20','#FAIR');
INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt","Direction","SenderType","EmployeeID","TrackingCode") VALUES
 (1,1,'L-0001','2026-03-14 14:20','In','Customer',NULL,'#FAIR'),
 (1,1,'L-0002','2026-03-14 14:31','Out','Staff',5,NULL);                  -- May sends the brochure from the inbox
UPDATE transaction."Inquiry" SET "ResponseTimeMinutes"=11 WHERE "InquiryID"=1;   -- system-set from ChatMessage
-- @step 2026-03-24
-- 21 Mar consultation (CRM calendar); 24 Mar quote 95,000 (12.5% of 800k, minus a fair discount)
UPDATE transaction."Inquiry" SET "ConsultationDate"='2026-03-21 10:00' WHERE "InquiryID"=1;
UPDATE transaction."Inquiry" SET "QuotedDate"='2026-03-24 16:00',"QuotedAmount"=95000,"Status"='Quoted' WHERE "InquiryID"=1;
-- @step 2026-04-02
-- 2 Apr they sign
INSERT INTO transaction."Event" VALUES (1,1,1,1,'2027-01-16',350,'Medium',95000,'Confirmed','2026-04-02 11:00');
UPDATE transaction."Inquiry" SET "Status"='Converted',"ClosedDate"='2026-04-02 11:00',"ClosedBy"='Sales' WHERE "InquiryID"=1;
UPDATE master."Customer" SET "Address"='88 Soi Phahonyothin 32, Chatuchak, Bangkok' WHERE "CustomerID"=1;
INSERT INTO transaction."EventEmployeeAssignment"("EventID","EmployeeID","AssignmentRole","AssignedDate","WorkloadHours","IsLead","Status") VALUES
 (1,1,'Lead Planner','2026-04-03',80,'Y','Active'),(1,2,'Planner','2026-04-03',35,'N','Active');

-- =============== PART 2: Nok & Bank (Facebook, night, goes silent) ===============
-- @step 2026-03-21
INSERT INTO master."Customer" VALUES (2,NULL,'2026-03-20 21:40');
INSERT INTO master."CustomerContact"("CustomerID","ContactName","IsPrimaryContact") VALUES (2,'Nok (Facebook name)','Y');
INSERT INTO master."CustomerChannelAccount" VALUES (2,2,'Messenger','PSID-27718304','Nok','2026-03-20 21:40',NULL);
INSERT INTO transaction."Inquiry"("InquiryID","CustomerID","ChannelID","IndicativePackageID","InquiryDate","RequestedEventDate","BudgetRange","Status")
 VALUES (2,2,2,3,'2026-03-20 21:40','2026-12-12','300k-500k','New');
INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt","Direction","SenderType","EmployeeID") VALUES
 (2,2,'M-0001','2026-03-20 21:40','In','Customer',NULL),
 (2,2,'M-0002','2026-03-20 21:41','Out','Bot',NULL),          -- auto-greeting: not a response
 (2,2,'M-0003','2026-03-21 10:05','Out','Staff',5),
 (2,2,'M-0004','2026-03-21 10:20','In','Customer',NULL),
 (2,2,'M-0005','2026-03-21 10:34','Out','Staff',5);           -- package sheet sent; Nok never writes again
UPDATE transaction."Inquiry" SET "ContactedDate"='2026-03-21 10:05',"ResponseTimeMinutes"=745,"Status"='Contacted' WHERE "InquiryID"=2;
-- @step 2026-04-20  (30 days after Nok's last message the rule closes the inquiry)
UPDATE transaction."Inquiry" SET "Status"='Lost',"LostReason"='No reply',"ClosedDate"='2026-04-20 10:20',"ClosedBy"='AutoRule' WHERE "InquiryID"=2;

-- =============== PART 3: Mint & Jay ask for 13 Feb 2027 (capacity decision) ===============
-- @step 2026-11-21
-- Fri 20 Nov 2026 13:02: they tap a TikTok link; LINE opens with "#TIKTOK" typed in.
INSERT INTO master."Customer" VALUES (3,NULL,'2026-11-20 13:02');
INSERT INTO master."CustomerContact"("CustomerID","ContactName","IsPrimaryContact") VALUES (3,'Mint (LINE name)','Y');
INSERT INTO master."CustomerChannelAccount" VALUES (3,3,'LINE','U77c1d2e3f4a5','Mint','2026-11-20 13:02','#TIKTOK');
INSERT INTO transaction."Inquiry"("InquiryID","CustomerID","ChannelID","IndicativePackageID","InquiryDate","RequestedEventDate","BudgetRange","Status")
 VALUES (3,3,4,2,'2026-11-20 13:02','2027-02-13','1M-1.5M','New');
INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt","Direction","SenderType","EmployeeID","TrackingCode") VALUES
 (3,3,'L-0101','2026-11-20 13:02','In','Customer',NULL,'#TIKTOK'),
 (3,3,'L-0102','2026-11-20 13:09','Out','Staff',5,NULL);
UPDATE transaction."Inquiry" SET "ContactedDate"='2026-11-20 13:09',"ResponseTimeMinutes"=7,"Status"='Contacted' WHERE "InquiryID"=3;
-- May checks the weekly workload view for 8-14 Feb: coordinator Beam is already above 100% that week
-- and two weddings are on the 13th. Decision: do not take a third wedding that day; offer Sat 20 Feb (also auspicious).
INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt","Direction","SenderType","EmployeeID") VALUES
 (3,3,'L-0103','2026-11-20 15:30','Out','Staff',5),
 (3,3,'L-0104','2026-11-21 09:15','In','Customer',NULL);      -- "the 13th is the date the monk gave us, sorry"
UPDATE transaction."Inquiry" SET "Status"='Lost',"LostReason"='Date unavailable',"ClosedDate"='2026-11-21 09:30',"ClosedBy"='Sales' WHERE "InquiryID"=3;
-- ETL run #1:  psql -v asof=2026-11-23 -f WeddingOrganizer_DW_v6_lean_ETL.sql

-- =============== PART 4: management decision after the capacity review ===============
-- Thu 10 Dec 2026: the weekly view shows Beam over 100% in the week of 8 Feb. Decision: hire a
-- freelance coordinator for Jan-Mar (20 h/week) and give her the two 14 Feb weddings.
INSERT INTO master."Employee" VALUES (6,3,'F006','Fah','F.','Freelance','2027-01-04',20,10,'Active');
UPDATE transaction."EventEmployeeAssignment" SET "EmployeeID"=6, "AssignedDate"='2027-01-04'
 WHERE "EmployeeID"=3 AND "EventID" IN (103,104);
-- ETL run #2:  psql -v asof=2026-12-14 -f WeddingOrganizer_DW_v6_lean_ETL.sql
-- Result: Beam's week of 8 Feb drops to 24 h (55%), but Fah is at 24 h against her 20 h (120%).
-- The overload moved; it did not go away.

-- =============== PART 4b: better split ===============
-- Tue 15 Dec 2026: ops gives event 103 (14 Feb, 480 guests) back to Beam; Fah keeps event 104 only.
UPDATE transaction."EventEmployeeAssignment" SET "EmployeeID"=3, "AssignedDate"='2027-01-12'
 WHERE "EmployeeID"=6 AND "EventID"=103;
-- ETL run #2b: psql -v asof=2026-12-15 -f WeddingOrganizer_DW_v6_lean_ETL.sql
-- Result: Beam 36 h (82%), Fah 12 h (60%). The coordinators are fine.
-- Still red: event staff Arm, 48 h against 40 (120%). He is on all four weddings, two per day,
-- which he cannot physically do. Decision: book day-rate crew for 13-14 Feb.
-- (The weekly view shows the overload; a same-day double booking is something the CRM
-- assignment screen should block, not something the warehouse is built to catch.)

-- =============== PART 5: a contract change ===============
-- Wed 20 Jan 2027: Nina moves to a 36-hour week from February.
UPDATE master."Employee" SET "StandardWeeklyHours"=36 WHERE "EmployeeID"=2;
-- ETL run #3:  psql -v asof=2027-02-01 -f WeddingOrganizer_DW_v6_lean_ETL.sql
