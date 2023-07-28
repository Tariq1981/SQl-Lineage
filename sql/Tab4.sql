CREATE OR REPLACE TABLE temp_tab4 /*PARTITION BY rpt_dt*/ AS
(
    SELECT colT1,colT2,salessT
    FROM
    (
        SELECT X.colT11 as colT1,X.colT22 as colT2,X.salessT1 as salessT,Y.rpt_dt,Y.RPT_YEAR
        FROM
        (
            SELECT x.col11+y.col1 as colT11,x.col1-y.col2 colT22,sum(y.sales) salessT1
            FROM SRC1 x
            INNER JOIN SRC2 y
            ON x.ID=y.ID
            GROUP BY 1
        ) X
        LEFT OUTER JOIN
        (
            SELECT col1,sales,CURRENT_DATE as rpt_dt,EXTRACT(YEAR FROM CURRENT_DATE) RPT_YEAR
            FROM SRC2 y
        ) Y
        ON X.colTT = Y.col1
    ) X
);


INSERT INTO Tab4(colT1,colT2,salessT)
     SELECT colT1,colT2,salessT
     FROM temp_tab4;