--GCP DDL

CREATE TABLE testRoxDB.ddl_person
(
  BusinessEntityID INT64 NOT NULL,
  PersonType STRING,
  NameStyle INT64,
  Title STRING,
  FirstName STRING,
  MiddleName STRING,
  LastName STRING,
  Suffix STRING,
  EmailPromotion INT64,
  AdditionalContactInfo STRING,
  Demographics STRING,
  rowguid STRING,
  ModifiedDate TIMESTAMP
)

