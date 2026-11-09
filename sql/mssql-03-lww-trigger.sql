USE ProductionDB;
GO
-- Last-writer-wins: an update applies only if it beats the current row on
-- higher version, then newer last_updated_at, then higher last_updated_by.
-- Fires for application writes as well as replicated ones. Keep the rule in
-- sync with postgres-03-lww-trigger.sql, or the nodes can pick different winners.
--
-- Covers INSERT too because the sink's MERGE has both branches, and SQL Server
-- requires INSTEAD OF triggers for every action in a MERGE if any has one.
--
-- Timestamps compare at DATETIME2(6) to match PostgreSQL's microseconds. Events
-- from SQL Server still arrive with millisecond precision, so ties within one
-- millisecond can resolve differently on each node.
CREATE OR ALTER TRIGGER dbo.tr_Orders_LWW ON dbo.Orders
INSTEAD OF INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- This trigger joins inserted to deleted on the key, so it can't apply a key change.
    IF UPDATE(OrderID) AND EXISTS (SELECT 1 FROM deleted)
        THROW 50001, 'dbo.Orders: changing OrderID is not supported', 1;

    INSERT INTO dbo.Orders
        (OrderID, CustomerID, OrderStatus, OrderTotal, last_updated_at, last_updated_by, version)
    SELECT i.OrderID, i.CustomerID, i.OrderStatus, i.OrderTotal, i.last_updated_at, i.last_updated_by, i.version
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.OrderID = i.OrderID);

    UPDATE o
    SET CustomerID      = i.CustomerID,
        OrderStatus     = i.OrderStatus,
        OrderTotal      = i.OrderTotal,
        last_updated_at = i.last_updated_at,
        last_updated_by = i.last_updated_by,
        version         = i.version
    FROM dbo.Orders o
    JOIN inserted i ON i.OrderID = o.OrderID
    JOIN deleted  d ON d.OrderID = o.OrderID
    WHERE i.version > d.version
       OR (i.version = d.version
           AND CAST(i.last_updated_at AS DATETIME2(6)) > CAST(d.last_updated_at AS DATETIME2(6)))
       OR (i.version = d.version
           AND CAST(i.last_updated_at AS DATETIME2(6)) = CAST(d.last_updated_at AS DATETIME2(6))
           AND i.last_updated_by COLLATE Latin1_General_BIN2 > d.last_updated_by COLLATE Latin1_General_BIN2);
END;
GO
