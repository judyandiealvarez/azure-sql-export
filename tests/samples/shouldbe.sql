ALTER VIEW d.test
AS 
WITH 
    dd AS
    (
        SELECT
            id,
            name
        FROM prod
    ),
    FINAL AS
    (
        SELECT
            *
        FROM dd
    )
    
SELECT
    id
FROM FINAL