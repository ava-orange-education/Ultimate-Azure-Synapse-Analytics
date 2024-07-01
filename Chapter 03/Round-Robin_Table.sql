CREATE TABLE Orders_Staging (
    OrderID INT,
    CustomerID INT,
    OrderDate DATETIME,
    TotalAmount DECIMAL(10,2)
)
WITH (DISTRIBUTION = ROUND_ROBIN);
