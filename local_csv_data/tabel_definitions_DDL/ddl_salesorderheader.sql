CREATE TABLE testRoxDB.ddl_salesorderheader
(
  SalesOrderID INT64 NOT NULL,
  RevisionNumber INT64,
  OrderDate TIMESTAMP,
  DueDate TIMESTAMP,
  ShipDate TIMESTAMP,
  Status INT64,
  OnlineOrderFlag INT64,
  SalesOrderNumber STRING,
  PurchaseOrderNumber STRING,
  AccountNumber STRING,
  CustomerID INT64,
  SalesPersonID INT64,
  TerritoryID INT64,
  BillToAddressID INT64,
  ShipToAddressID INT64,
  ShipMethodID INT64,
  CreditCardID INT64,
  CreditCardApprovalCode STRING,
  CurrencyRateID INT64,
  SubTotal FLOAT64,
  TaxAmt FLOAT64,
  Freight FLOAT64,
  TotalDue FLOAT64,
  Comment STRING,
  rowguid STRING,
  ModifiedDate TIMESTAMP
)

