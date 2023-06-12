# QE_ATF
This is Tiger QE's Automated Testing Framework for Data Migration and ETL Testing and connect with pipelines.

**Features of ATF:**

1. Testcase Creation Automated  using Source to Target document Meta data  along with well formatted PDF Test Results.
2. Count Verification : Compare record counts between source and target data sets.
3. Content Verification : Compare every field of source and target data to check for data types, data structure, data corruption, data precession.
4. Business Rules Verification: Verify if simple  business rules are applied successfully using automatic Testcase Generation
5. Custom (Manual) Rules: Verify that complex business rules are applied successfully  using Manual Query Generation.
6. Schema Verification : Compare the dataset schema (structure) between source and target data sets.
7. Regression Testing: Compare before and after datasets in case of source system change.(RedShift or Oracle)
8. ATF can be used for Unit Testing and System Integration Testing(QA)
9. Incase of Failed Testcases Provides Precise sample records which did not match along with columns which did not match with source and target column values

**Architecture Details:**

![My Image](/docs/images/ATF_Architecture.jpg)