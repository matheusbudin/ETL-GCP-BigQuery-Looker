CREATE TABLE testRoxDB.Product
(
  ProductID INT64 NOT NULL,
  Name STRING,
  ProductNumber STRING,
  MakeFlag INT64,
  FinishedGoodsFlag INT64,
  Color STRING,
  SafetyStockLevel INT64,
  ReorderPoint INT64,
  StandardCost FLOAT64,
  ListPrice FLOAT64,
  Size STRING,
  SizeUnitMeasureCode STRING,
  WeightUnitMeasureCode STRING,
  Weight FLOAT64,
  DaysToManufacture INT64,
  ProductLine STRING,
  Class STRING,
  Style STRING,
  ProductSubcategoryID INT64,
  ProductModelID INT64,
  SellStartDate TIMESTAMP,
  SellEndDate TIMESTAMP,
  DiscontinuedDate TIMESTAMP,
  rowguid STRING,
  ModifiedDate TIMESTAMP
)

