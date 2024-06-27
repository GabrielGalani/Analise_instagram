DROP TABLE IF EXISTS FTbAccount;
DROP TABLE IF EXISTS FTbMidiasInsights;
DROP TABLE IF EXISTS FTbMidias;
DROP TABLE IF EXISTS FTbAccountLifetimeInsights;
DROP TABLE IF EXISTS FTbAccountDayInsights;
DROP TABLE IF EXISTS DTbDescricaoInsights;
DROP TABLE IF EXISTS DTCarrossel;
DROP TABLE IF EXISTS TbAccount;
DROP TABLE IF EXISTS DTbMidias;


SELECT 
    fk.name AS ForeignKeyName,
    tp.name AS ParentTable,
    cp.name AS ParentColumn,
    tr.name AS ReferencedTable,
    cr.name AS ReferencedColumn
FROM 
    sys.foreign_keys AS fk
INNER JOIN 
    sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN 
    sys.tables AS tp ON fkc.parent_object_id = tp.object_id
INNER JOIN 
    sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
INNER JOIN 
    sys.tables AS tr ON fkc.referenced_object_id = tr.object_id
INNER JOIN 
    sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
WHERE 
    tr.name = 'TbAccount';

ALTER TABLE DTbMidias DROP CONSTRAINT FK__DTbMidias__id_ac__01D345B0;

DROP TABLE IF EXISTS TbAccount;
DROP TABLE IF EXISTS DTbMidias;