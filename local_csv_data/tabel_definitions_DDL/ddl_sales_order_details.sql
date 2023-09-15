CREATE TABLE testRoxDB.SalesOrderDetail
(
  SalesOrderID INT64,
  SalesOrderDetailID INT64 NOT NULL,
  CarrierTrackingNumber STRING,
  OrderQty INT64,
  ProductID INT64,
  SpecialOfferID INT64,
  UnitPrice FLOAT64,
  UnitPriceDiscount FLOAT64,
  LineTotal FLOAT64,
  rowguid STRING,
  ModifiedDate TIMESTAMP
)

