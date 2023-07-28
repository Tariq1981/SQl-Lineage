CREATE OR REPLACE TABLE Tab7 /*PARTITION BY rpt_dt*/ AS
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


INSERT INTO Tab8(col1,cl2,sales)
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
    ) X;


CREATE OR REPLACE TABLE Tab7 /*PARTITION BY rpt_dt*/ AS
(
    SELECT X.*
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

CREATE OR REPLACE TABLE Tab7 /*PARTITION BY rpt_dt*/ AS
(
    SELECT X.*
    FROM
    (
        SELECT X.*,Y.*
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



CREATE OR REPLACE TABLE Tab6 /*PARTITION BY rpt_dt*/ AS
(
    SELECT * except(rpt_dt,RPT_YEAR)
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




CREATE OR REPLACE TABLE Tab1 /*PARTITION BY rpt_dt*/ AS
(
    SELECT x.colT11 as colT1,X.colT22 as colT2,X.salessT1 as salessT,Y.rpt_dt,Y.RPT_YEAR
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
);
INSERT INTO Tab2(col21,col22,sales)
    SELECT col1 as col21,col2 as col22,salessT as sales
    FROM
    (
      SELECT colT1*2 as col1,colT2 as col2,salessT * 10 as salessT
      FROM
      (
        SELECT *
        FROM Tab1
        WHERE colT1>=2
      ) X
    ) X;

CREATE OR REPLACE TABLE Tab11 AS
(
     With X AS
     (
        SELECT col21*10 as col1,col22 * 20 as col2,sales * 100 as sales100
        FROM Tab2
     ),
     Y AS
     (
        SELECT colT1+40 as col1,colT2 + 20 as col2,salessT + 100 as sales100
        FROM Tab1
     ),
     Z AS
     (
        SELECT *
        FROM Y
     )
     SELECT YY.col1,XX.col2,XX.sales100 + YY.sales100 as sales
     FROM X XX
     INNER JOIN Z YY
     ON XX.col1 = YY.col1 AND XX.col2 = YY.col2
);

INSERT INTO Tab3(col1,col2,sum_sales)
     With X AS
     (
        SELECT col21*10 as col1,col22 * 20 as col2,sales * 100 as sales100
        FROM Tab2
     ),
     Y AS
     (
        SELECT colT1+40 as col1,colT2 + 20 as col2,salessT + 100 as sales100
        FROM Tab1
     ),
     Z AS
     (
        SELECT *
        FROM Y
     )
     SELECT YY.col1,XX.col2,XX.sales100 + YY.sales100 as sales
     FROM X XX
     INNER JOIN Z YY
     ON XX.col1 = YY.col1 AND XX.col2 = YY.col2;

INSERT INTO Tab4
    SELECT col21*10 as col1,col22 * 20 as col2,sales * 100 as sales100
    FROM Tab2;


INSERT INTO Tab5
    SELECT * except(col22)
    FROM Tab2;