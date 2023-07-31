CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_d_subscriber_main_pstg` /*###partition by calendar_day_dt*/ PARTITION BY DATE(end_dttm)
AS (
SELECT
    dss.*,
    /*###RANK() OVER(PARTITION BY dss.dw_subs_id, dss.calendar_day_dt
      ORDER BY
        (CASE
          WHEN (UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active) OR UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active_not_reported)) AND
         ( UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
            OR UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_mobile)) THEN 2
             WHEN UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_inactive)  AND dss.subs_dconn_rpt_dt= dss.calendar_day_dt
            AND( UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_fixed) OR UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_mobile))
        THEN 1
        ELSE 0
      END )DESC , dss.start_dttm DESC) rnk,*/
    subs_rpt_stat_cd,
    subs_rpt_sub_stat_cd,
    dw_uniq_subs_rpt_stat_id,
    bl0.business_line_lvl_2_cd,
    bl0.business_line_lvl_1_cd,
    bl0.dw_uniq_business_line_id,
    bl0.business_line_lvl_3_cd,
    ssr.subs_stat_rsn_lvl_3_cd,
    bl0.start_dttm AS bl0_start_dttm, --###
    bl0.end_dttm AS bl0_end_dttm, --###
    rs.start_dttm AS rs_start_dttm, --###
    rs.end_dttm AS rs_end_dttm, --###
    ssr.start_dttm AS ssr_start_dttm, --###
    ssr.end_dttm AS ssr_end_dttm --###
  FROM (
    SELECT
      s.dw_uniq_subs_id,
      s.dw_subs_id,
      s.dw_sub_subs_id,
      s.dw_subs_rpt_stat_id,
      s.start_dttm,
      s.end_dttm,
      s.subs_conn_num,
      s.dw_cust_acct_id,
      s.dw_sub_cust_acct_id,
      s.dw_sf_position_id,
      s.dw_subs_stat_id,
      s.dw_subs_stat_rsn_id,
      s.dw_subs_barring_stat_id,
      s.dw_business_line_id,
      s.dw_rpt_tariff_plan_id,
      s.dw_physical_line_id,
      s.subs_preactivation_rpt_dt,
      s.subs_reconnection_rpt_dt,
      s.subs_rgstn_dt,
      s.subs_barring_dt,
      s.subs_conn_rpt_dt,
      s.subs_dconn_rpt_dt,
      s.dw_prod_id,
      s.dw_sub_prod_id,
      /*###c.calendar_day_dt,*/
      s.subs_conn_dt,
      s.subs_dconn_dt,
	  s.ss_cd  --V12 changes
    FROM
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber` s
    --x >= DATE(s.start_dttm) AND x <= DATE(s.end_dttm)
      /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) c
                on timestamp(calendar_day_dt || ' 23:59:59') between s.start_dttm and s.end_dttm*/
   WHERE s.end_dttm IS NOT NULL AND s.start_dttm <= timestamp(y || ' 23:59:59') AND s.end_dttm >= timestamp(x || ' 00:00:00') --###
    ) dss
  INNER JOIN
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_business_line` bl0
  ON
    dss.dw_business_line_id =bl0.dw_business_line_id
   -- AND x >= DATE(bl0.start_dttm)
   -- AND x <= DATE(bl0.end_dttm)
   /*###and timestamp(dss.calendar_day_dt || ' 23:59:59')  between bl0.start_dttm and bl0.end_dttm*/
   and (UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
            OR UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_mobile)OR UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_rgu)
     /*###AND bl0.end_dttm is not null*/
        )
   AND bl0.end_dttm IS NOT NULL AND bl0.start_dttm <= timestamp(y || ' 23:59:59') AND bl0.end_dttm >= timestamp(x || ' 00:00:00') --###
INNER JOIN
  (
    SELECT
      dw_subs_rpt_stat_id,
      subs_rpt_stat_cd,
      subs_rpt_sub_stat_cd,
      dw_uniq_subs_rpt_stat_id,
      start_dttm,end_dttm
    FROM
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_reporting_status`
    where end_dttm is not null
      AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
    ) rs
  ON dss.dw_subs_rpt_stat_id =rs.dw_subs_rpt_stat_id
    /*###and timestamp(dss.calendar_day_dt || ' 23:59:59') between rs.start_dttm and rs.end_dttm */
INNER JOIN
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_status_reason` ssr
  ON dss.dw_subs_stat_rsn_id = ssr.dw_subs_stat_rsn_id
    /*###AND timestamp(calendar_day_dt || ' 23:59:59') between ssr.start_dttm AND ssr.end_dttm*/
 AND ssr.end_dttm is not null
 AND ssr.start_dttm <= timestamp(y || ' 23:59:59') AND ssr.end_dttm >= timestamp(x || ' 00:00:00') --###
    );
----------- ENDS CREATING 1 MAIN TABLE THAT IS TO BE USED FOR BASE ALL DIMENSIONS & RGU COUNTS AS WELL------

--************************************************************************************************************--


-----************************END**************************-----------
-------------D SUBSCRIBER TEMP STAGING TABLE CREATION ---------------


-----************************END**************************-----------
-------------D SUBSCRIBER TEMP STAGING TABLE CREATION ---------------

----********************BEGIN TEMP DEVICE ASSOCIATION TABLE********************-------------------

CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_deviceassoc_pstag` PARTITION BY DATE(end_dttm) AS --###
(
SELECT
        * /*###EXCEPT(rnk)*/
      FROM (
                SELECT
                dw_subs_id,
                dw_sub_subs_id,
                is_dvc_with_main_sim,
                dw_dvc_id,
                start_dttm, --###
                end_dttm --###
                /*###calendar_day_dt,*/
                /*###row_number() OVER(PARTITION BY dw_subs_id,dw_sub_subs_id,calendar_day_dt ORDER BY is_dvc_with_main_sim DESC, start_dttm DESC, end_dttm desc) rnk*/ --- row_number handling
                FROM
                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_device_association`
                /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) c
                on timestamp(calendar_day_dt || ' 23:59:59') between start_dttm and end_dttm AND end_dttm is not null*/
                WHERE end_dttm IS NOT NULL AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###

            )
            /*###WHERE rnk=1 */
);
----********************BEGIN TEMP DEVICE ASSOCIATION TABLE********************-------------------


----********************BEGIN TEMP DEVICE CATEGORY ASSOCIATION TABLE********************-------------------
CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_dvccatassoc_pstag` PARTITION BY DATE(end_dttm) AS --###
    SELECT
        dw_dvc_id,
        dw_dvc_cat_id,
        start_dttm,
        end_dttm
        /*###ROW_NUMBER() OVER(PARTITION BY dw_dvc_id ,calendar_day_dt ORDER BY START_DTTM DESC, dw_dvc_cat_id desc) rn,*/
        /*###calendar_day_dt*/
        FROM
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_device_category_association`
        /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) c
                on timestamp(calendar_day_dt || ' 23:59:59') between start_dttm and end_dttm*/
        WHERE
        --x >= DATE(start_dttm)
        --AND x <= DATE(end_dttm)
        --AND
              UPPER(dvc_cat_ss_type) IN UNNEST(v_dvc_cat_for_reporting) AND end_dttm is not null
          AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
    ;
----********************END TEMP DEVICE CATEGORY ASSOCIATION TABLE ********************--------------------

----********************BEGIN TEMP CUSTOMER ACCOUNT TABLE CREATION********************-------------------

CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_cust_acct_pstag` PARTITION BY DATE(end_dttm) AS --###
(
    SELECT
       ca.dw_cust_acct_id,
      ca.dw_sub_cust_acct_id,
      ca.dw_uniq_cust_acct_id,
      ca.dw_cust_id,
      ca.dw_sub_cust_id, --###
      cs0_dw_cust_seg_id,
      cs0_dw_uniq_cust_seg_id,
      /*###rca0.dw_geo_id AS rca0_dw_geo_id,*/
      /*###g2_dw_uniq_geo_id,*/
      c0.dw_uniq_cust_id AS c0_dw_uniq_cust_id,
      c0.dw_cust_type_id AS c0_dw_cust_type_id,
      c0.dw_sector_id AS c0_dw_sector_id,
      ct0_dw_uniq_cust_type_id,
      sec0_dw_unique_sector_id,
      cac0.cust_acct_cat_lvl_1_cd, --###
      IFNULL(ca.start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS start_dttm, --###
      IFNULL(ca.end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS end_dttm, --###
      IFNULL(c0.c_start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS c_start_dttm, --###
      IFNULL(c0.c_end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS c_end_dttm, --###
      --###IFNULL(rca0.rca_start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS rca_start_dttm, --###
      --###IFNULL(rca0.rca_end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS rca_end_dttm, --###
      IFNULL(c0.cs_start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS cs_start_dttm, --###
      IFNULL(c0.cs_end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS cs_end_dttm, --###
      IFNULL(c0.cc_start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS cc_start_dttm, --###
      IFNULL(c0.cc_end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS cc_end_dttm, --###
      IFNULL(c0.ct_start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS ct_start_dttm, --###
      IFNULL(c0.ct_end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS ct_end_dttm, --###
      IFNULL(c0.sec_start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS sec_start_dttm, --###
      IFNULL(c0.sec_end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS sec_end_dttm, --###
      --###IFNULL(rca0.g_start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS g_start_dttm, --###
      --###IFNULL(rca0.g_end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS g_end_dttm, --###
      IFNULL(cac0.start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS cac_start_dttm, --###
      IFNULL(cac0.end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS cac_end_dttm --###
      /*###ca.calendar_day_dt,*/
    /*###RANK() OVER (partition BY ca.dw_cust_acct_id, ca.calendar_day_dt order by
      case when UPPER(cac0.cust_acct_cat_lvl_1_cd) IN UNNEST(v_cust_acc_main_cat) then 1 else 0 end  desc) as rnk*/
    FROM (
            SELECT
                dw_cust_acct_id,
                dw_sub_cust_acct_id,
                dw_uniq_cust_acct_id,
                dw_cust_id,
                dw_cust_acct_cat_id,
                dw_sub_cust_id,
                start_dttm, --###
                end_dttm --###
                /*###calendar_day_dt*/
            FROM
                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_account`
                /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) c
                on timestamp(calendar_day_dt || ' 23:59:59') between start_dttm and end_dttm AND end_dttm is not null*/
           WHERE end_dttm IS NOT NULL AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###

        )ca
    LEFT JOIN (
      SELECT
        c.dw_cust_id,
        c.dw_sub_cust_id,
        c.dw_uniq_cust_id,
        c.dw_cust_type_id,
        c.dw_sector_id,
        ct0.dw_uniq_cust_type_id AS ct0_dw_uniq_cust_type_id,
        sec0.dw_uniq_sector_id AS sec0_dw_unique_sector_id,
        cs0.dw_cust_seg_id AS cs0_dw_cust_seg_id,
        cs0.dw_uniq_cust_seg_id AS cs0_dw_uniq_cust_seg_id,
        c.start_dttm AS c_start_dttm, --###
        c.end_dttm AS c_end_dttm, --###
        cs0.start_dttm AS cs_start_dttm, --###
        cs0.end_dttm AS cs_end_dttm, --###
        cc0.start_dttm AS cc_start_dttm, --###
        cc0.end_dttm AS cc_end_dttm, --###
        ct0.start_dttm AS ct_start_dttm, --###
        ct0.end_dttm AS ct_end_dttm, --###
        sec0.start_dttm AS sec_start_dttm, --###
        sec0.end_dttm AS sec_end_dttm --###
        /*###c.calendar_day_dt*/
      FROM (
        SELECT
          dw_cust_id,
          dw_sub_cust_id,
          dw_uniq_cust_id,
          dw_cust_type_id,
          dw_sector_id,
          dw_cust_seg_id,
          dw_cust_cat_id,
          start_dttm, --###
          end_dttm --###
          /*###calendar_day_dt*/
        FROM
          `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer`
          /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                on timestamp(calendar_day_dt || ' 23:59:59') between start_dttm and end_dttm AND end_dttm is not null*/
       WHERE end_dttm IS NOT NULL AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
        ) c
      INNER JOIN
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_segment` cs0
      ON
        c.dw_cust_seg_id=cs0.dw_cust_seg_id
        /*###AND timestamp(calendar_day_dt || ' 23:59:59') between cs0.start_dttm  AND cs0.end_dttm AND cs0.end_dttm is not null*/
     AND cs0.end_dttm IS NOT NULL AND cs0.start_dttm <= timestamp(y || ' 23:59:59') AND cs0.end_dttm >= timestamp(x || ' 00:00:00') --###
      INNER JOIN
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_category` cc0
      ON
        c.dw_cust_cat_id= cc0.dw_cust_cat_id
        /*###AND timestamp(calendar_day_dt || ' 23:59:59') between cc0.start_dttm AND cc0.end_dttm*/
        AND UPPER(cc0.cust_cat_lvl_1_cd) IN UNNEST(v_cust_main_cat) AND cc0.end_dttm is not null
        AND cc0.start_dttm <= timestamp(y || ' 23:59:59') AND cc0.end_dttm >= timestamp(x || ' 00:00:00') --###
      INNER JOIN
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_type` ct0
      ON
        ct0.dw_cust_type_id=c.dw_cust_type_id
        /*###AND  timestamp(calendar_day_dt || ' 23:59:59') between ct0.start_dttm AND ct0.end_dttm*/
        AND ct0.end_dttm is not null
        AND ct0.start_dttm <= timestamp(y || ' 23:59:59') AND ct0.end_dttm >= timestamp(x || ' 00:00:00') --###
      INNER JOIN
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_sector` sec0
      ON
        sec0.dw_sector_id=c.dw_sector_id
        /*###AND  timestamp(calendar_day_dt || ' 23:59:59') between sec0.start_dttm AND sec0.end_dttm*/ AND sec0.end_dttm is not null
     AND sec0.start_dttm <= timestamp(y || ' 23:59:59') AND sec0.end_dttm >= timestamp(x || ' 00:00:00') --###

    )c0
    ON
      c0.dw_cust_id= ca.dw_cust_id
      /*###AND  c0.calendar_day_dt = ca.calendar_day_dt*/
      /*### Start
   LEFT JOIN (
                SELECT
                 rca.dw_cust_id,
                rca.dw_sub_cust_id,
                rca.dw_geo_id,
                g2.dw_uniq_geo_id AS g2_dw_uniq_geo_id,
                calendar_day_dt
                FROM
                (
                    SELECT * EXCEPT(r) FROM
                    (
                        SELECT
                             dw_cust_id,
                            dw_sub_cust_id,
                            dw_geo_id,
                            rank() over( partition by  dw_cust_id,calendar_day_dt order by start_dttm desc ,end_dttm desc, dw_cust_addr_id desc) r,
                            calendar_day_dt
                        FROM
                            `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.r_customer_address`
                            inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                on timestamp(calendar_day_dt || ' 23:59:59') between start_dttm and end_dttm
                        WHERE
                            UPPER(addr_type) IN UNNEST(v_main_address) and end_dttm is not null
                    ) where r = 1
                ) rca
                INNER JOIN
                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_geography` g2
                ON
                rca.dw_geo_id=g2.dw_geo_id
                AND timestamp(calendar_day_dt || ' 23:59:59') BETWEEN g2.start_dttm AND g2.end_dttm
               AND g2.end_dttm is not null
            )rca0
    ON
      rca0.dw_cust_id= ca.dw_cust_id
      AND rca0.dw_sub_cust_id= ca.dw_sub_cust_id
      AND rca0.calendar_day_dt = ca.calendar_day_dt
          End */
    INNER JOIN
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_account_category` cac0
    ON
      cac0.dw_cust_acct_cat_id= ca.dw_cust_acct_cat_id
      /*###AND timestamp(ca.calendar_day_dt || ' 23:59:59') BETWEEN cac0.start_dttm AND cac0.end_dttm*/
      and cac0.end_dttm is not null
      AND cac0.start_dttm <= timestamp(y || ' 23:59:59') AND cac0.end_dttm >= timestamp(x || ' 00:00:00') --###
      --AND UPPER(cac0.cust_acct_cat_lvl_1_cd) IN UNNEST(v_cust_acc_main_cat)
    );


----********************END TEMP CUSTOMER ACCOUNT TABLE CREATION********************-------------------


----********************BEGIN TEMP CUSTOMER AGREEMENT TABLE********************-------------------
CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_f_cust_agreement_pstag` PARTITION BY rpt_dt AS --###
    SELECT
                 dw_subs_id,
                cust_agmt_term_rpt_dt,
                dw_cust_agmt_id,
                dw_sf_position_id,
                dw_cust_agmt_class_id,
                with_commitment,
                new_agmt_ind,
                rpt_dt/*###,*/
                /*###calendar_day_dt,*/
                /*###rank() over(partition by dw_subs_id , calendar_day_dt order by cust_agmt_term_rpt_dt desc,dw_cust_agmt_id desc) r*/ -- this is done to remove duplicate
            FROM
                `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_customer_agreement_base_semantic_d`
                /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                on calendar_day_dt = rpt_dt*/
            WHERE
                with_commitment = 1
                --AND DATE(rpt_dt) = x
              AND rpt_dt BETWEEN x AND y --###
                ;

----********************END TEMP CUSTOMER AGREEMENT TABLE********************-------------------

 ----********************BEGIN TEMP PORT DETAILS TABLE********************-------------------
CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_port_req_details_pstag` PARTITION BY DATE(end_dttm) AS ( --###
/*This temp table is to get Port details of the subscriber.
Join to D Subscriber on subscriber id

Tables used:
1- D Port Request Detail       3- D Port Request Type       5- D Port Request Status Reason
2- D Port Request              4- D Port Request Status     6- D Operator

Before join use [row number] = 1 for d port request detail.
To ensure we are picking <Priority> record only ( 1 record for subs ), for perticular processing date.
<Priority> 1 if [Use prs0.port_req_sts_lvl1_name] = Completed.
2.In Progress
3.Cancelled
4. Rejected
5. Closed
6. On Hold ( Sort in Asc order and pick first )*/
    SELECT
      * --###EXCEPT(r1)
    FROM
    (
      SELECT
        prd.dw_subs_id,
        prd.dw_sub_subs_id,
        prd.start_dttm,
        prd.end_dttm,
        prd.dw_port_req_id AS prd_dw_port_req_id, --###
		pr0.port_req_completed_dttm AS prd_port_req_completed_dttm,--NPP-19714
        pr0.dw_port_req_stat_id AS pr0_dw_port_req_stat_id,
        pr0.dw_donor_operator_id AS pr0_dw_donor_operator_id,
        pr0.dw_port_req_stat_rsn_id AS pr0_dw_port_req_stat_rsn_id,
        pr0.dw_receiving_operator_id AS pr0_dw_receiving_operator_id,
        prs0_dw_uniq_port_req_stat_id,
        prsr0_dw_uniq_port_req_stat_rsn_id,
        o0_dw_uniq_operator_id,
        prs0_port_req_stat_lvl_1_cd,
        prsr0_port_req_stat_rsn_lvl_1_cd,
        prt0_port_req_type_cd,
        pr0.priority AS pr0_priority,
        /*###row_number () OVER(PARTITION BY prd.dw_subs_id, prd.calendar_day_dt ORDER BY pr0.priority ASC, prd.dw_port_req_id) r1,*/
        pr0.pr_start_dttm, --###
        pr0.pr_end_dttm, --###
        IFNULL(pr0.o_start_dttm, TIMESTAMP('1900-01-01 00:00:00')) AS o_start_dttm, --###
        IFNULL(pr0.o_end_dttm, TIMESTAMP('9999-12-31 23:59:59')) AS o_end_dttm, --###
        pr0.prt_start_dttm, --###
        pr0.prt_end_dttm, --###
        pr0.prs_start_dttm, --###
        pr0.prs_end_dttm, --###
        pr0.prsr_start_dttm, --###
        pr0.prsr_end_dttm --###
        /*###prd.calendar_day_dt*/
      FROM (
                SELECT
                 dw_subs_id,
                dw_sub_subs_id,
                start_dttm,
                end_dttm,
                dw_port_req_id/*###,*/
                /*###calendar_day_dt*/
                FROM
                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request_detail`
                /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                timestamp(calendar_day_dt || ' 23:59:59') BETWEEN  start_dttm and end_dttm AND end_dttm is not null*/
                --x >= DATE(start_dttm)
                --AND x <= DATE(end_dttm)
                WHERE end_dttm IS NOT NULL AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
            )prd
      INNER JOIN (
                    SELECT
                    pr.dw_port_req_id,
                    pr.dw_port_req_stat_id,
                    pr.dw_donor_operator_id,
					pr.port_req_completed_dttm,--NPP-19714
                    pr.dw_port_req_stat_rsn_id,
                    pr.dw_receiving_operator_id,
                    prs0.port_req_stat_lvl_1_cd AS prs0_port_req_stat_lvl_1_cd,
                    prsr0.port_req_stat_rsn_lvl_1_cd AS prsr0_port_req_stat_rsn_lvl_1_cd,
                    prs0.dw_uniq_port_req_stat_id AS prs0_dw_uniq_port_req_stat_id,
                    prsr0.dw_uniq_port_req_stat_rsn_id AS prsr0_dw_uniq_port_req_stat_rsn_id,
                    o0.dw_uniq_operator_id AS o0_dw_uniq_operator_id,
                    prt0.port_req_type_cd AS prt0_port_req_type_cd,
                    CASE
                        WHEN UPPER(prs0.port_req_stat_lvl_1_cd) IN UNNEST (v_mnp_app_port_req_stat_p1) THEN 1
                        WHEN UPPER(prs0.port_req_stat_lvl_1_cd) IN UNNEST (v_mnp_app_port_req_stat_p2) THEN 2
                        WHEN UPPER(prs0.port_req_stat_lvl_1_cd) IN UNNEST (v_mnp_app_port_req_stat_p3) THEN 3
                        WHEN UPPER(prs0.port_req_stat_lvl_1_cd) IN UNNEST (v_mnp_app_port_req_stat_p4) THEN 4
                        WHEN UPPER(prs0.port_req_stat_lvl_1_cd) IN UNNEST (v_mnp_app_port_req_stat_p5) THEN 5
                        WHEN UPPER(prs0.port_req_stat_lvl_1_cd) IN UNNEST (v_mnp_app_port_req_stat_p6) THEN 6
                        ELSE 99
                    END
                    AS priority,
                    pr_start_dttm, --###
                    pr_end_dttm, --###
                    o0.start_dttm AS o_start_dttm, --###
                    o0.end_dttm AS o_end_dttm, --###
                    prt0.start_dttm AS prt_start_dttm, --###
                    prt0.end_dttm AS prt_end_dttm, --###
                    prs0.start_dttm AS prs_start_dttm, --###
                    prs0.end_dttm AS prs_end_dttm, --###
                    prsr0.start_dttm AS prsr_start_dttm, --###
                    prsr0.end_dttm AS prsr_end_dttm --###
                    /*###pr.calendar_day_dt*/
                    FROM (
                            SELECT
                                dw_port_req_id,
                                dw_port_req_stat_id,
								port_req_completed_dttm,--NPP-19714
                                dw_port_req_type_id,
                                dw_donor_operator_id,
                                dw_port_req_stat_rsn_id,
                                dw_receiving_operator_id,
                                start_dttm AS pr_start_dttm, --###
                                end_dttm AS pr_end_dttm --###
                                /*###calendar_day_dt*/
                            FROM
                                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request`
                            /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                timestamp(calendar_day_dt || ' 23:59:59') BETWEEN  start_dttm and end_dttm  AND end_dttm is not null*/
                            --WHERE
                                --x >= DATE(start_dttm)
                                --AND x <= DATE(end_dttm)
                            WHERE end_dttm IS NOT NULL AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
                        ) pr
                    LEFT JOIN  --workaround to left join, originally it is inner join
                    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_operator` o0
                    ON
                    pr.dw_receiving_operator_id= o0.dw_operator_id
                    /*###AND timestamp(calendar_day_dt || ' 23:59:59') BETWEEN o0.start_dttm AND o0.end_dttm*/
                    AND o0.end_dttm is not null
                    --AND x >= DATE(o0.start_dttm)
                    --AND x <= DATE(o0.end_dttm)
                    AND o0.start_dttm <= timestamp(y || ' 23:59:59') AND o0.end_dttm >= timestamp(x || ' 00:00:00') --###
                    INNER JOIN
                    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request_type` prt0
                    ON
                    pr.dw_port_req_type_id= prt0.dw_port_req_type_id
                    /*###AND timestamp(calendar_day_dt || ' 23:59:59') BETWEEN prt0.start_dttm AND prt0.end_dttm*/
                    AND prt0.end_dttm is not null
                    --AND x >= DATE(prt0.start_dttm)
                    --AND x <= DATE(prt0.end_dttm)
                    AND prt0.start_dttm <= timestamp(y || ' 23:59:59') AND prt0.end_dttm >= timestamp(x || ' 00:00:00') --###
                    INNER JOIN
                    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request_status` prs0
                    ON
                    pr.dw_port_req_stat_id= prs0.dw_port_req_stat_id
                    /*###AND  timestamp(calendar_day_dt || ' 23:59:59') BETWEEN prs0.start_dttm AND prs0.end_dttm*/
                    AND prs0.end_dttm is not null
                    --AND x >= DATE(prs0.start_dttm)
                    --AND x <= DATE(prs0.end_dttm)
                    AND prs0.start_dttm <= timestamp(y || ' 23:59:59') AND prs0.end_dttm >= timestamp(x || ' 00:00:00') --###
                    INNER JOIN
                    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request_status_reason` prsr0
                    ON
                    pr.dw_port_req_stat_rsn_id= prsr0.dw_port_req_stat_rsn_id
                    /*###AND  timestamp(calendar_day_dt || ' 23:59:59') BETWEEN prsr0.start_dttm AND prsr0.end_dttm*/
                    AND prsr0.end_dttm is not null
                    --AND x >= DATE(prsr0.start_dttm)
                    --AND x <= DATE(prsr0.end_dttm)
                    AND prsr0.start_dttm <= timestamp(y || ' 23:59:59') AND prsr0.end_dttm >= timestamp(x || ' 00:00:00') --###
                )pr0
      ON
        prd.dw_port_req_id=pr0.dw_port_req_id
        /*###and prd.calendar_day_dt = pr0.calendar_day_dt*/
        )
    --###WHERE
      --###r1=1
    );
 ----********************END TEMP PORT DETAILS TABLE********************-------------------
/*
Join F Activity Base Semantic D to D Subscriber on subscriber id and date

Table used:
1- F Activity Base Semantic D
Pick the 1st record, i.e. the latest one or the latest available
This ensures selecting the latest available records when multiple are available.
Data must never be filtered out via this join and records must not be duplicated.

*/
 ----********************BEGIN ACTIVITY BASE SEM TABLE********************-------------------

 CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_activitybasesem_pstag` PARTITION BY rpt_dt AS --###
(SELECT * /*###EXCEPT (rnk)*/
    FROM
    (
    SELECT dw_subs_id,
    --v12 dw_sub_subs_id,
    dw_uniq_subs_id, --###
    last_chargeable_event_dt,
    rpt_dt --###
    /*###calendar_day_dt,*/
    /*###RANK() OVER(PARTITION BY dw_subs_id,dw_sub_subs_id,calendar_day_dt  ORDER BY rpt_dt DESC) rnk*/
    FROM
    `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_activity_base_semantic_d`
    /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON rpt_dt BETWEEN DATE_SUB(calendar_day_dt, INTERVAL 7 day) and calendar_day_dt*/
            where rpt_dt is not null
              AND rpt_dt BETWEEN DATE_SUB(x, INTERVAL 7 day) AND y --###
    )
    /*###WHERE rnk= 1*/
    );
  ----********************END TEMP ACTIVITY BASE SEM  TABLE********************-------------------

----********************BEGIN TEMP PHYSICAL LINE TABLE********************-------------------
 CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_subssegassoc_pstag` PARTITION BY DATE(end_dttm) AS --###
(
            SELECT
                 dw_subs_id,
                dw_sub_subs_id,
                dw_subs_seg_id,
                start_dttm, --###
                end_dttm --###
                /*###calendar_day_dt,*/
                /*###row_number() over(partition by dw_subs_id,dw_sub_subs_id,calendar_day_dt  order by start_dttm desc, end_dttm desc, dw_subs_seg_id desc) rn*/
            FROM
                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_segment_association`
                /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm*/
            WHERE
                 UPPER(TRIM(subs_seg_ss_type)) IN UNNEST(v_subs_seg_for_reporting) AND end_dttm is not null
              AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
        );
----********************END TEMP PHYSICAL LINE TABLE********************-------------------

----********************BEGIN TEMP PHYSICAL LINE TABLE********************-------------------

  CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_physicalline_pstag` PARTITION BY DATE(end_dttm) AS --###
(SELECT * /*###EXCEPT(pl_rn)*/ FROM
                        (SELECT
                         dw_physical_line_id,
                        dw_uniq_physical_line_id,
                        dw_fixed_line_type_id,
                        start_dttm, --###
                        end_dttm --###
                        /*###calendar_day_dt,*/
                        /*###row_number() over(partition by dw_physical_line_id,calendar_day_dt order by start_dttm desc, end_dttm desc) pl_rn*/ -- WORKAROUND TO HANDLE DUPLICATE RECORDS IN D_PHYSICAL LINE
                    FROM
                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_physical_line`
                        /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm AND end_dttm is not null*/
                   WHERE end_dttm IS NOT NULL AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
                    )
                        /*###WHERE pl_rn = 1*/
    );


----------------------MIGRATION CALCULATION----------------------
 /*###CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_subs_migration_stag` AS
(
            SELECT
            s.dw_subs_id,
            SUM(CASE
                WHEN UPPER(subs_rpt_sub_stat_cd) IN UNNEST(v_sub_stat_migrated) THEN 1
                ELSE 0
                END
                ) AS count_migrated,
            SUM(CASE
                WHEN (UPPER(subs_stat_rsn_lvl_3_cd) IN UNNEST(v_subs_stat_rsn_add_mig) OR UPPER(subs_stat_rsn_lvl_3_cd) IN UNNEST(v_subs_stat_rsn_addtech_mig)) THEN 1
                ELSE 0
                END
                ) AS count_reason_addr,
            SUM(CASE
                WHEN (UPPER(subs_stat_rsn_lvl_3_cd) IN UNNEST(v_subs_stat_rsn_tech_mig) OR UPPER(subs_stat_rsn_lvl_3_cd) IN UNNEST(v_subs_stat_rsn_addtech_mig)) THEN 1
                    ELSE 0
                    END
                ) AS count_reason_tech,
            calendar_day_dt
            FROM
            `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_d_subscriber_main_pstg` s
            WHERE
            ((UPPER(s.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                AND s.subs_conn_dt = calendar_day_dt) OR (UPPER(s.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_deactive_not_reported)
                AND s.subs_dconn_dt = calendar_day_dt))
            GROUP BY
            s.dw_subs_id , calendar_day_dt
    );*/

 ---------- BEGIN: Creating temp staging tables for tariff_plan, business_line and subscriber_segment FOR ENRICH HISTORY to avaoid re processing-------

CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_tp_pstag` PARTITION BY DATE(end_dttm) AS --###
    (   SELECT
        dw_tariff_plan_id,
        dw_uniq_tariff_plan_id,
        start_dttm, --###
        end_dttm --###
        /*###calendar_day_dt*/
        FROM
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_tariff_plan`
        /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm*/
        WHERE end_dttm IS NOT NULL
          AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
    );

CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_bl_pstag` PARTITION BY DATE(end_dttm) AS --###
    (   SELECT
        dw_business_line_id,
        dw_uniq_business_line_id,
        business_line_lvl_2_cd,
        start_dttm, --###
        end_dttm --###
        /*###calendar_day_dt*/
        FROM
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_business_line`
        /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm  */
        WHERE end_dttm IS NOT NULL
          AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###

    );

CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_subseg_pstag` PARTITION BY DATE(end_dttm) AS --###
    (   SELECT
        dw_subs_seg_id,
        dw_uniq_subs_seg_id,
        subs_seg_lvl_1_cd,
        start_dttm, --###
        end_dttm --###
        /*###calendar_day_dt*/
        FROM
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_segment`
        /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm */
                WHERE end_dttm is not null
                  AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###

    );

CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_sf_pos_pstag` PARTITION BY DATE(end_dttm) AS --###
    (   SELECT
        dw_sf_position_id,
        dw_uniq_sf_position_id,
        start_dttm, --###
        end_dttm --###
        /*###calendar_day_dt*/
        FROM
            `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_sales_force_position`
        /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm*/
                 WHERE end_dttm is not null
                   AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
    );

CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_sf_type_pstag` PARTITION BY DATE(end_dttm) AS --###
    (   SELECT
        dw_sf_type_id,
        dw_uniq_sf_type_id,
        start_dttm, --###
        end_dttm --###
        /*###calendar_day_dt*/
        FROM
            `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_sales_force_type`
        /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm*/
                 WHERE end_dttm is not null
                   AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
    );
---------- END: Creating temp staging tables for tariff_plan, business_line and subscriber_segment for ENRICH HISTORY to avaoid re processing-------

--### Start
--------- Start: Creating staging table for past details on subscriber using daily and monthly F SUBSCRIBER BASE SEMANTIC

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_hist_subs_base_sem_d_and_m_pstag` PARTITION BY rpt_dt AS
   SELECT  dw_subs_id
          ,dw_curr_business_line_id
          ,dw_prev_business_line_id
          ,subs_agmt_last_term_dt
          ,last_business_line_change_dt
          ,dw_curr_rpt_tariff_plan_id
          ,dw_prev_rpt_tariff_plan_id
          ,last_rpt_tariff_plan_change_dt
          ,dw_curr_subs_seg_id
          ,dw_prev_subs_seg_id
          ,last_subs_seg_change_dt
          ,dw_acq_sf_position_id
          ,dw_acq_sf_type_id
          ,dw_last_retention_sf_position_id
          ,dw_last_retention_sf_type_id
          ,dw_first_acquired_prod_id
          ,dw_sub_first_acquired_prod_id
          ,is_port_in
		  ,port_in_completion_dt --NPP-19714
		  ,port_out_completion_dt --NPP-19714
          ,dw_port_in_operator_id
          ,subs_last_retention_dt
          ,dw_curr_fixed_line_type_id
          ,dw_prev_fixed_line_type_id
          ,last_fixed_line_type_change_dt
          ,converge_change_dt
          ,rgu_bundle_change_dt
          ,play_count_change_dt
          ,dw_cust_id
          ,rpt_dt
          ,play_count_change
          ,is_in_closing_base
          ,is_in_active_base
          ,inactive_to_active_change_dt
          ,dw_curr_prim_dvc_id
          ,dw_prev_prim_dvc_id
          ,dw_acq_business_line_id
          ,dw_acq_rpt_tariff_plan_id
          ,dw_acq_subs_seg_id
          ,dw_dconn_business_line_id
          ,dw_dconn_subs_seg_id
          ,dw_dconn_rpt_tariff_plan_id
          ,dw_dconn_stat_rsn_id
          ,last_prim_dvc_change_dt
     FROM `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_subscriber_base_semantic_d`
    WHERE DATE_TRUNC(x, MONTH) < x
	  AND rpt_dt BETWEEN DATE_TRUNC(x, MONTH) AND DATE_SUB(x, INTERVAL 1 DAY)
   UNION ALL
   SELECT  dw_subs_id
          ,dw_curr_business_line_id
          ,dw_prev_business_line_id
          ,subs_agmt_last_term_dt
          ,last_business_line_change_dt
          ,dw_curr_rpt_tariff_plan_id
          ,dw_prev_rpt_tariff_plan_id
          ,last_tariff_plan_change_dt AS last_rpt_tariff_plan_change_dt
          ,dw_curr_subs_seg_id
          ,dw_prev_subs_seg_id
          ,last_subs_seg_change_dt
          ,dw_acq_sf_position_id
          ,dw_acq_sf_type_id
          ,dw_retention_sf_position_id AS dw_last_retention_sf_position_id
          ,dw_retention_sf_type_id AS dw_last_retention_sf_type_id
          ,dw_first_acquired_prod_id
          ,dw_first_acquired_sub_prod_id AS dw_sub_first_acquired_prod_id
          ,is_port_in
		  ,port_in_completion_dt --NPP-19714
		  ,port_out_completion_dt --NPP-19714
          ,dw_port_in_operator_id
          ,subs_last_retention_dt
          ,dw_curr_fixed_line_type_id
          ,dw_prev_fixed_line_type_id
          ,last_fixed_line_type_change_dt
          ,converge_change_dt
          ,rgu_bundle_change_dt
          ,play_count_change_dt
          ,dw_cust_id
          ,rpt_month AS rpt_dt
          ,play_count_change
          ,is_in_closing_base
          ,is_in_active_base
          ,inactive_to_active_change_dt
          ,dw_curr_prim_dvc_id
          ,dw_prev_prim_dvc_id
          ,dw_acq_business_line_id
          ,dw_acq_rpt_tariff_plan_id
          ,dw_acq_subs_seg_id
          ,dw_dconn_business_line_id
          ,dw_dconn_subs_seg_id
          ,dw_dconn_rpt_tariff_plan_id
          ,dw_dconn_stat_rsn_id
          ,last_prim_dvc_change_dt
     FROM `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_subscriber_base_semantic_m`
    WHERE rpt_month BETWEEN DATE_SUB(x, INTERVAL reconn_expiry_months MONTH) AND DATE_SUB(x, INTERVAL 1 DAY)
;

--------- End: Creating staging table for past details on subscriber using daily and monthly F SUBSCRIBER BASE SEMANTIC
--### End

-----------------------------------BEGIN: BASE ALL DIMENSION WITHOUT AGREEMENT RELTED COLUMNS CREATION----------------


--### Start
CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_deviceassoc_rn_pstag` PARTITION BY DATE(end_dttm) AS
   SELECT  *
          ,ROW_NUMBER() OVER(PARTITION BY dw_subs_id, dw_sub_subs_id ORDER BY is_dvc_with_main_sim DESC, start_dttm DESC, end_dttm DESC) AS dvc_with_main_sim_flg
     FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_deviceassoc_pstag`
    WHERE x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
;

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_dvccatassoc_rn_pstag` PARTITION BY DATE(end_dttm) AS
   SELECT  *
          ,ROW_NUMBER() OVER(PARTITION BY dw_dvc_id ORDER BY START_DTTM DESC, dw_dvc_cat_id DESC) AS rn
     FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_dvccatassoc_pstag`
    WHERE x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
;

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_subssegassoc_rn_pstag` PARTITION BY DATE(end_dttm) AS
   SELECT  *
          ,ROW_NUMBER() OVER(PARTITION BY dw_subs_id, dw_sub_subs_id ORDER BY start_dttm DESC, end_dttm DESC, dw_subs_seg_id DESC) AS rn
     FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_subssegassoc_pstag`
    WHERE end_dttm IS NOT NULL AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
;

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_cust_acct_rnk_pstag` PARTITION BY DATE(end_dttm) AS
   SELECT  *
          ,RANK() OVER (PARTITION BY dw_cust_acct_id ORDER BY CASE WHEN UPPER(cust_acct_cat_lvl_1_cd) IN UNNEST(v_cust_acc_main_cat) THEN 1 ELSE 0 END  DESC) AS rnk
     FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_cust_acct_pstag`
;

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_physicalline_rn_pstag` PARTITION BY DATE(end_dttm) AS
   SELECT  *
          ,ROW_NUMBER() OVER(PARTITION BY dw_physical_line_id ORDER BY start_dttm DESC, end_dttm DESC, dw_uniq_physical_line_id) pl_rn
     FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_physicalline_pstag`
    WHERE end_dttm IS NOT NULL AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
;

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_sub_contact_detail_rn_pstag` PARTITION BY DATE(end_dttm) AS
    SELECT g4.dw_geo_id AS g4_dw_geo_id,
           rscd.dw_subs_id,
           rscd.dw_sub_subs_id,
           g4.dw_uniq_geo_id AS g4_dw_uniq_geo_id,
           rscd.start_dttm,
           rscd.end_dttm
      FROM (SELECT postal_code_cd,
                   dw_subs_id,
                   dw_sub_subs_id,
                   start_dttm,
                   end_dttm
              FROM `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.r_subscriber_contact_detail`
             WHERE end_dttm IS NOT NULL
               AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY dw_subs_id, dw_sub_subs_id ORDER BY start_dttm DESC, postal_code_cd DESC, dw_subs_cntc_id) = 1
           ) rscd
    INNER JOIN `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_geography` g4
            ON rscd.postal_code_cd = g4.postal_code_cd
           AND g4.end_dttm is not null
           AND x BETWEEN DATE(g4.start_dttm) AND DATE(g4.end_dttm)
;

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_cust_acct_contact_detail_rn_pstag` PARTITION BY DATE(end_dttm) AS
    SELECT rcacd.dw_cust_acct_id,
           rcacd.dw_sub_cust_acct_id,
           g3.dw_geo_id AS g3_dw_geo_id,
           g3.dw_uniq_geo_id AS g3_dw_uniq_geo_id,
           rcacd.start_dttm,
           rcacd.end_dttm
      FROM (SELECT postal_code_cd,
                   dw_cust_acct_id,
                   dw_sub_cust_acct_id,
                   start_dttm,
                   end_dttm
              FROM `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.r_customer_account_contact_detail`
             WHERE end_dttm IS NOT NULL
               AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY dw_cust_acct_id, dw_sub_cust_acct_id ORDER BY start_dttm DESC, postal_code_cd DESC, dw_cust_acct_cntc_id) = 1
           )rcacd INNER JOIN `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_geography` g3
                          ON rcacd.postal_code_cd = g3.postal_code_cd
                         AND g3.end_dttm is not null
                         AND x BETWEEN DATE(g3.start_dttm) AND DATE(g3.end_dttm)
;

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_cust_contact_detail_rn_pstag` PARTITION BY DATE(rccd_end_dttm) AS
    SELECT  rccd.dw_cust_id as rccd_dw_cust_id
           ,rccd.dw_sub_cust_id as rccd_dw_sub_cust_id
           ,g5.dw_geo_id AS g5_dw_geo_id
           ,g5.dw_uniq_geo_id AS g5_dw_uniq_geo_id
		   ,rccd.start_dttm AS rccd_start_dttm
		   ,rccd.end_dttm AS rccd_end_dttm
      FROM (SELECT  postal_code_cd
                   ,dw_cust_id
                   ,dw_sub_cust_id
                   ,start_dttm
                   ,end_dttm
              FROM `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.r_customer_contact_detail`
             WHERE end_dttm IS NOT NULL AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
           QUALIFY ROW_NUMBER() OVER (PARTITION BY dw_cust_id, dw_sub_cust_id ORDER BY start_dttm DESC, postal_code_cd DESC, dw_cust_cntc_id) = 1

           ) rccd
             INNER JOIN `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_geography` g5
                     ON rccd.postal_code_cd = g5.postal_code_cd
                    AND g5.end_dttm IS NOT NULL AND x BETWEEN DATE(g5.start_dttm) AND DATE(g5.end_dttm)
;
--### End

CREATE OR REPLACE TABLE
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_basealldims_pstag` PARTITION BY rpt_dt AS --###
SELECT
  *,
  CASE
    WHEN COUNTIF(with_commitment=1) OVER(PARTITION BY dw_subs_id) >0 THEN 1
    ELSE 0
  END AS is_in_contract_commitment,
  CASE
    WHEN is_in_active_base = 1 THEN NULL
    ELSE DATE_ADD(last_chargeable_event_dt, INTERVAL 30 day)
  END AS active_to_inactive_change_dt,
  CASE
   WHEN dconn_ind = 1 THEN dw_curr_business_line_id
   ELSE to_hex(sha256("*NA"))
  END AS dw_dconn_business_line_id,
  CASE
   WHEN dconn_ind = 1 THEN dw_curr_rpt_tariff_plan_id
   ELSE to_hex(sha256("*NA"))
  END AS dw_dconn_rpt_tariff_plan_id,
   CASE
   WHEN dconn_ind = 1 THEN dw_curr_subs_seg_id
   ELSE to_hex(sha256("*NA"))
  END AS dw_dconn_subs_seg_id,
   CASE
   WHEN dconn_ind = 1 THEN dw_subs_stat_rsn_id
   ELSE to_hex(sha256("*NA"))
  END AS dw_dconn_stat_rsn_id,
FROM (
  SELECT
    DISTINCT x AS rpt_dt,
    s0.dw_subs_id AS dw_subs_id,
    s0.dw_uniq_subs_id AS dw_uniq_subs_id,
    s0.subs_conn_num AS subs_conn_num,
    s0.dw_cust_acct_id AS dw_cust_acct_id,
    ca0.dw_uniq_cust_acct_id AS dw_uniq_cust_acct_id,
    s0.dw_subs_stat_id AS dw_subs_stat_id,
    ss0.dw_uniq_subs_stat_id AS dw_uniq_subs_stat_id,
    s0.dw_subs_stat_rsn_id AS dw_subs_stat_rsn_id,
    ssr0.dw_uniq_subs_stat_rsn_id AS dw_uniq_subs_stat_rsn_id,
    s0.dw_subs_rpt_stat_id AS dw_subs_rpt_stat_id,
    s0.dw_uniq_subs_rpt_stat_id AS dw_uniq_subs_rpt_stat_id,
    s0.dw_subs_barring_stat_id AS dw_subs_barring_stat_id,
    sbs0.dw_uniq_subs_barring_stat_id AS dw_uniq_subs_barring_stat_id,
    s0.dw_business_line_id AS dw_curr_business_line_id,
    s0.dw_uniq_business_line_id AS dw_uniq_current_business_line_id,
    s0.dw_rpt_tariff_plan_id AS dw_curr_rpt_tariff_plan_id,
    tp0.dw_uniq_tariff_plan_id AS dw_uniq_current_rpt_tariff_plan_id,
    ssa0.dw_subs_seg_id AS dw_curr_subs_seg_id,
    sseg0_dw_uniq_subs_seg_id AS dw_uniq_current_subs_seg_id,
    c0_dw_cust_type_id AS dw_cust_type_id,
    ct0_dw_uniq_cust_type_id AS dw_uniq_cust_type_id,
    cs0_dw_cust_seg_id AS dw_cust_seg_id,
    cs0_dw_uniq_cust_seg_id AS dw_uniq_cust_seg_id,
    c0_dw_sector_id AS dw_cust_sector_id,
    sec0_dw_unique_sector_id AS dw_uniq_cust_sector_id,
    s0.dw_physical_line_id AS dw_physical_line_id,
    da0.dw_dvc_id AS dw_curr_primary_dvc_id,
    d0_dw_uniq_dvc_id AS dw_uniq_curr_primary_dvc_id,
    dca0_dw_dvc_cat_id AS dw_curr_primary_dvc_cat_id,
    dc0_dw_uniq_dvc_cat_id AS dw_uniq_curr_primary_dvc_cat_id,
    COALESCE(g0_dw_geo_id, g4_dw_geo_id) AS dw_subs_geo_id, --v12 ###
    COALESCE(g0_dw_uniq_geo_id, g4_dw_uniq_geo_id) AS dw_uniq_subs_geo_id, --v12 ###
    COALESCE(g1_dw_geo_id, g3_dw_geo_id) AS dw_cust_acct_geo_id, --v12 ###
    COALESCE(g1_dw_uniq_geo_id, g3_dw_uniq_geo_id) AS dw_uniq_cust_acct_geo_id, --v12 ###
    COALESCE(g2_dw_geo_id, g5_dw_geo_id) AS dw_cust_geo_id, --v12 ###
    COALESCE(g2_dw_uniq_geo_id, g5_dw_uniq_geo_id) AS dw_uniq_cust_geo_id, --v12 ###
    pr0_dw_port_req_stat_id AS dw_port_req_stat_id,
    prs0_dw_uniq_port_req_stat_id AS dw_uniq_port_req_stat_id,
    pr0_dw_port_req_stat_rsn_id AS dw_port_req_stat_rsn_id,
    prsr0_dw_uniq_port_req_stat_rsn_id AS dw_uniq_port_req_stat_rsn_id,
    s0.subs_preactivation_rpt_dt AS subs_preactivation_rpt_dt,
    s0.subs_conn_rpt_dt AS subs_conn_rpt_dt,
    s0.subs_dconn_rpt_dt AS subs_dconn_rpt_dt,
    s0.subs_reconnection_rpt_dt AS subs_reconnection_rpt_dt,
    s0.subs_rgstn_dt AS subs_rgstn_dt,
    s0.subs_barring_dt AS subs_barring_dt,
    cagrbs0.dw_sf_position_id AS dw_last_retention_sf_position_id,
    sfp0.dw_sf_type_id AS dw_last_retention_sf_type_id,
    with_commitment,
    s0.business_line_lvl_2_cd,
    CASE
      WHEN ifnull(ca0.dw_cust_id,to_hex(sha256("*NA")))=to_hex(sha256("*NA")) THEN "CUST_"||s0.dw_subs_id
      ELSE ca0.dw_cust_id
    END AS dw_cust_id,
    CASE
      WHEN ifnull(ca0.dw_cust_id,to_hex(sha256("*NA")))=to_hex(sha256("*NA")) THEN "CUST_"||s0.dw_uniq_subs_id
      ELSE c0_dw_uniq_cust_id
    END AS dw_uniq_cust_id,
    CASE
      WHEN ifnull(ca0.dw_cust_id,to_hex(sha256("*NA")))=to_hex(sha256("*NA")) THEN 1
      ELSE 0
    END AS is_dummy_cust,
    CASE
      WHEN s0.subs_conn_rpt_dt = DATE(s0.start_dttm) AND s0.subs_conn_rpt_dt = x AND UPPER(TRIM(prt0_port_req_type_cd)) IN UNNEST(v_port_req_type_in) AND (UPPER(prs0_port_req_stat_lvl_1_cd) IN UNNEST(v_port_req_stat_success) OR UPPER(prsr0_port_req_stat_rsn_lvl_1_cd) IN UNNEST(v_port_req_stat_rsn_success))
        AND x /*###= s0.calendar_day_dt */ BETWEEN DATE(s0.start_dttm) AND DATE(s0.end_dttm)
        THEN 1
      ELSE 0
    END AS is_port_in,
	--NPP-19714<
	CASE
	  WHEN --UPPER(TRIM(prt0_port_req_type_cd)) IN UNNEST(v_port_req_type_in) AND
      (UPPER(prs0_port_req_stat_lvl_1_cd) IN UNNEST(v_port_req_stat_success) OR UPPER(prsr0_port_req_stat_rsn_lvl_1_cd) IN UNNEST(v_port_req_stat_rsn_success))
		THEN DATE(port_in_completed_dttm) ELSE CAST(NULL AS DATE) --imposible to get target2
	END AS port_in_completion_dt,
	CASE
	  WHEN --UPPER(TRIM(prt0_port_req_type_cd)) IN UNNEST(v_port_req_type_out) AND
      (UPPER(prs0_port_req_stat_lvl_1_cd) IN UNNEST(v_port_req_stat_success) OR UPPER(prsr0_port_req_stat_rsn_lvl_1_cd) IN UNNEST(v_port_req_stat_rsn_success))
		THEN DATE(port_out_completed_dttm) ELSE CAST(NULL AS DATE) --imposible to get target2
	END AS port_out_completion_dt,
    -->NPP-19714
    IF(UPPER(TRIM(prt0_port_req_type_cd)) IN UNNEST(v_port_req_type_out),
      1,
      0 ) AS is_port_out,
    CASE
      WHEN s0.subs_conn_rpt_dt = DATE(s0.start_dttm) AND s0.subs_conn_rpt_dt = x AND UPPER(TRIM(prt0_port_req_type_cd)) IN UNNEST(v_port_req_type_in) AND (UPPER(prs0_port_req_stat_lvl_1_cd) IN UNNEST(v_port_req_stat_success) OR UPPER(prsr0_port_req_stat_rsn_lvl_1_cd) IN UNNEST(v_port_req_stat_rsn_success))
      AND   x /*###= s0.calendar_day_dt*/ BETWEEN DATE(s0.start_dttm) AND DATE(s0.end_dttm) THEN pr0_dw_donor_operator_id
      ELSE to_hex(sha256("*NA"))
    END AS dw_port_in_operator_id,
    IF (UPPER(TRIM(prt0_port_req_type_cd)) IN UNNEST(v_port_req_type_out),
      pr0_dw_receiving_operator_id,
      to_hex(sha256("*NA"))) AS dw_port_out_operator_id,
    IF (UPPER(TRIM(prt0_port_req_type_cd)) IN UNNEST(v_port_req_type_out),
      o0_dw_uniq_operator_id,
      to_hex(sha256("*NA"))) AS dw_uniq_port_out_operator_id,
    cust_agmt_term_rpt_dt AS subs_agmt_last_term_dt,
    abs0.last_chargeable_event_dt AS last_chargeable_event_dt,
    (CASE
        WHEN count_migrated > 0 and count_reason_addr > 0
        THEN 1
        ELSE 0
     END
    )AS migr_of_addr_ind,
    (CASE
        WHEN count_migrated > 0 and count_reason_tech > 0
        THEN 1
        ELSE 0
     END
    )AS migr_of_tech_ind,
    (CASE
        WHEN s0.subs_conn_rpt_dt= DATE(s0.start_dttm)
        AND s0.subs_conn_rpt_dt=x
        AND UPPER(s0.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
        THEN 1
        ELSE 0
     END
    ) AS gross_add_ind,
    (CASE
        WHEN s0.subs_dconn_rpt_dt= DATE(s0.start_dttm)
        AND s0.subs_dconn_rpt_dt=x
        AND UPPER(s0.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_inactive)
        THEN 1
        ELSE 0
     END
    )AS dconn_ind,
    (CASE
        WHEN s0.subs_reconnection_rpt_dt= DATE(s0.start_dttm)
        AND s0.subs_reconnection_rpt_dt=x
        AND UPPER(s0.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
        THEN 1
        ELSE 0
     END
    )AS reconnection_ind,
    (CASE
        WHEN abs0.last_chargeable_event_dt >=DATE_SUB(x, INTERVAL 30 day)
        AND UPPER(s0.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
        THEN 1
      ELSE 0
     END
    )AS is_in_active_base,
    (CASE
        WHEN UPPER(s0.subs_rpt_stat_cd) IN UNNEST(v_subs_stat_closing_base)
        THEN 1
        ELSE 0
     END
    )AS is_in_closing_base,
    CASE
      WHEN s0.subs_conn_rpt_dt = DATE(s0.start_dttm)
      AND s0.subs_conn_rpt_dt = x
      THEN s0.dw_sf_position_id
      ELSE NULL
    END AS dw_acq_sf_position_id,
    CASE
      WHEN s0.subs_conn_rpt_dt = DATE(s0.start_dttm)
      AND s0.subs_conn_rpt_dt = x
      THEN sft0_dw_sf_type_id
      ELSE NULL
    END AS dw_acq_sf_type_id,
    CASE
      WHEN s0.subs_conn_rpt_dt = DATE(s0.start_dttm)
      AND s0.subs_conn_rpt_dt = x
      THEN s0.dw_prod_id
      ELSE NULL
    END AS dw_first_acquired_prod_id,
    CASE
      WHEN s0.subs_conn_rpt_dt = DATE(s0.start_dttm)
      AND s0.subs_conn_rpt_dt = x
      THEN s0.dw_sub_prod_id
      ELSE NULL
    END AS dw_sub_first_acquired_prod_id,
    CASE
      WHEN UPPER(s0.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_inactive)
      OR COUNTIF( IF ((UPPER(cagrc0_cust_agmt_class_cd) IN UNNEST(v_agmt_class_retention) AND cagrbs0.new_agmt_ind = 1),
      cagrbs0.dw_cust_agmt_id, NULL) IS NOT NULL)
      OVER (PARTITION BY s0.dw_subs_id) =0
      THEN 0
      ELSE NULL
    END AS retention_ind,
    sseg0_subs_seg_lvl_1_cd AS subs_seg_lvl_1_cd,
    pl0.dw_uniq_physical_line_id AS dw_uniq_physical_line_id,
    pl0.dw_fixed_line_type_id AS dw_curr_fixed_line_type_id,
    pl0.dw_uniq_fixed_line_type_id AS dw_uniq_curr_fixed_line_type_id,
    COUNTIF( IF ((UPPER(cagrc0_cust_agmt_class_cd) IN UNNEST(v_agmt_class_retention) AND cagrbs0.new_agmt_ind = 1),
      cagrbs0.dw_cust_agmt_id, NULL) IS NOT NULL)
      OVER (PARTITION BY s0.dw_subs_id) as agreement_count,
	s0.ss_cd --v12 changes
  FROM
    (select dw_subs_id,
  dw_uniq_subs_id,
  subs_conn_num,
  dw_cust_acct_id,
  dw_sub_cust_acct_id,
  dw_sub_subs_id,
  dw_sf_position_id,
  dw_subs_stat_id,
  dw_subs_stat_rsn_id,
  dw_subs_rpt_stat_id,
  dw_subs_barring_stat_id,
  dw_business_line_id,
  dw_rpt_tariff_plan_id,
  dw_physical_line_id,
  subs_preactivation_rpt_dt,
  subs_reconnection_rpt_dt,
  subs_rgstn_dt,
  subs_barring_dt,
  subs_conn_rpt_dt,
  subs_dconn_rpt_dt,
  start_dttm,
  end_dttm,
  dw_prod_id,
  dw_sub_prod_id,
  subs_rpt_stat_cd,
  dw_uniq_business_line_id,
  business_line_lvl_2_cd,
  business_line_lvl_1_cd,
  subs_rpt_sub_stat_cd,
  dw_uniq_subs_rpt_stat_id,
  /*###calendar_day_dt*/
  ss_cd --v12 changes
    from
    /*###`vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_d_subscriber_main_pstg` */
    --### Start
          (SELECT dss.*
                  ,RANK() OVER(PARTITION BY dss.dw_subs_id
                                   ORDER BY (CASE WHEN (UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                                                    OR  UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active_not_reported))
                                                   AND (UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                                    OR  UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_mobile)) THEN 2
                                                  WHEN  UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_inactive)
                                                   AND  dss.subs_dconn_rpt_dt BETWEEN DATE(dss.start_dttm) AND DATE(dss.end_dttm)
                                                   AND (UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                                    OR  UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_mobile)) THEN 1
                                                  ELSE 0
                                              END) DESC
                                            ,dss.start_dttm DESC
                              ) rnke
             FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_d_subscriber_main_pstg` dss
            WHERE x BETWEEN DATE(dss.start_dttm) AND DATE(dss.end_dttm)
              AND x BETWEEN DATE(dss.bl0_start_dttm) AND DATE(dss.bl0_end_dttm)
              AND x BETWEEN DATE(dss.rs_start_dttm) AND DATE(dss.rs_end_dttm)
              AND x BETWEEN DATE(dss.ssr_start_dttm) AND DATE(dss.ssr_end_dttm)
          )T
    --### End
        where
        /*###calendar_day_dt = x and */
            ((UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active) OR UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active_not_reported)) OR ( UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_inactive)  AND subs_dconn_rpt_dt= /*###calendar_day_dt*/ x))
        AND  (UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_fixed) OR UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_mobile))
    and T.rnke = 1
    )s0

  /* This Join is added to get Migration of technology and address indicators.
  Tables Used: D Subscriber , D Subscriber Status Reason & D Subscriber Reporting Status
  */
  /*###LEFT JOIN  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_subs_migration_pstag` s9
    ON s0.dw_subs_id = s9.dw_subs_id
    AND s0.calendar_day_dt = s9.calendar_day_dt*/
    --### Start
  LEFT JOIN (SELECT s.dw_subs_id,
                    SUM(CASE WHEN UPPER(subs_rpt_sub_stat_cd) IN UNNEST(v_sub_stat_migrated) THEN 1
                             ELSE 0
                         END) AS count_migrated,
                    SUM(CASE WHEN (UPPER(subs_stat_rsn_lvl_3_cd) IN UNNEST(v_subs_stat_rsn_add_mig) OR UPPER(subs_stat_rsn_lvl_3_cd) IN UNNEST(v_subs_stat_rsn_addtech_mig)) THEN 1
                             ELSE 0
                         END) AS count_reason_addr,
                    SUM(CASE WHEN (UPPER(subs_stat_rsn_lvl_3_cd) IN UNNEST(v_subs_stat_rsn_tech_mig) OR UPPER(subs_stat_rsn_lvl_3_cd) IN UNNEST(v_subs_stat_rsn_addtech_mig)) THEN 1
                             ELSE 0
                         END) AS count_reason_tech
               FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_d_subscriber_main_pstg` s
              WHERE ((UPPER(s.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                AND         s.subs_conn_dt = x)
                 OR  (UPPER(s.subs_rpt_stat_cd) IN UNNEST(v_sub_stat_deactive_not_reported)
                AND         s.subs_dconn_dt = x))
                AND x BETWEEN DATE(s.start_dttm) AND DATE(s.end_dttm)
                AND x BETWEEN DATE(s.bl0_start_dttm) AND DATE(s.bl0_end_dttm)
                AND x BETWEEN DATE(s.rs_start_dttm) AND DATE(s.rs_end_dttm)
                AND x BETWEEN DATE(s.ssr_start_dttm) AND DATE(s.ssr_end_dttm)
             GROUP BY s.dw_subs_id
            ) s9
         ON s0.dw_subs_id = s9.dw_subs_id
    --### End

  LEFT JOIN (

/* This join is to get agreement related details of the subscriber.
Join to D Subscriber on subscriber id

Tables used:
1- F Customer Agreement Base Semantic D
2- D Customer Agreement Classification
3- D Sales Force Position

Step 1: Filter on agreements with commitment.
Step 2: filter on date
Step 3: aggregate by subscriber id

*/
    SELECT
      cagrbs.dw_subs_id,
      cagrbs.cust_agmt_term_rpt_dt,
      cagrbs.dw_cust_agmt_id,
      cagrbs.dw_sf_position_id,
      cagrbs.dw_cust_agmt_class_id,
      cagrbs.with_commitment,
      cagrbs.rpt_dt,
      cagrbs.new_agmt_ind,
      cagrc0.cust_agmt_class_cd AS cagrc0_cust_agmt_class_cd/*###,*/
      /*###cagrbs.calendar_day_dt*/
    FROM (
            /*###SELECT * from
            `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_f_cust_agreement_pstag` where r=1*/
            --### Start
            SELECT * EXCEPT (r)
              FROM (SELECT  *
                           ,RANK() OVER(PARTITION BY dw_subs_id, rpt_dt ORDER BY cust_agmt_term_rpt_dt DESC,dw_cust_agmt_id DESC) r
                      FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_f_cust_agreement_pstag`
                     WHERE rpt_dt = x
                   )
             WHERE r=1
            --### End
        )cagrbs
    LEFT JOIN -- workaround as left join due to sales force potential CR, originally it is inner join
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_sales_force_position` sfp1
    ON
      sfp1.dw_sf_position_id= cagrbs.dw_sf_position_id
      /*###AND timestamp(calendar_day_dt || ' 23:59:59') between sfp1.start_dttm AND sfp1.end_dttm AND sfp1.end_dttm is not null*/
      AND sfp1.end_dttm IS NOT NULL AND cagrbs.rpt_dt BETWEEN DATE(sfp1.start_dttm) AND DATE(sfp1.end_dttm) --###
      --AND x >= DATE(sfp1.start_dttm)
      --AND x <= DATE(sfp1.end_dttm)
    INNER JOIN
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_agreement_classification` cagrc0
    ON
      cagrc0.dw_cust_agmt_class_id = cagrbs.dw_cust_agmt_class_id
      /*###AND timestamp(calendar_day_dt || ' 23:59:59') between cagrc0.start_dttm AND cagrc0.end_dttm*/
      AND cagrc0.end_dttm is not null
      AND cagrbs.rpt_dt BETWEEN DATE(cagrc0.start_dttm) AND DATE(cagrc0.end_dttm) --###
      --AND x >= DATE(cagrc0.start_dttm)
      --AND x <= DATE(cagrc0.end_dttm)
    ) cagrbs0
  ON
    s0.dw_subs_id =cagrbs0.dw_subs_id
    /*###AND s0.calendar_day_dt = cagrbs0.calendar_day_dt*/
    AND cagrbs0.rpt_dt BETWEEN DATE(s0.start_dttm) AND DATE(s0.end_dttm) --###
  LEFT JOIN
--###`vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_port_req_details_pstag` prd0
   --### Start
   (--NPP-19714<
 SELECT tt.*,
		MAX(CASE WHEN UPPER (ttt.prt0_port_req_type_cd) IN UNNEST(v_port_req_type_in) THEN ttt.prd_port_req_completed_dttm END) AS port_in_completed_dttm ,
		MAX(CASE WHEN UPPER (ttt.prt0_port_req_type_cd) IN UNNEST(v_port_req_type_out) THEN ttt.prd_port_req_completed_dttm END) AS port_out_completed_dttm
	FROM
  -->NPP-19714
   (SELECT  *
      FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_port_req_details_pstag`
     WHERE end_dttm IS NOT NULL AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
       AND pr_end_dttm IS NOT NULL AND x BETWEEN DATE(pr_start_dttm) AND DATE(pr_end_dttm)
       AND o_end_dttm IS NOT NULL AND x BETWEEN DATE(o_start_dttm) AND DATE(o_end_dttm)
       AND prt_end_dttm IS NOT NULL AND x BETWEEN DATE(prt_start_dttm) AND DATE(prt_end_dttm)
       AND prs_end_dttm IS NOT NULL AND x BETWEEN DATE(prs_start_dttm) AND DATE(prs_end_dttm)
       AND prsr_end_dttm IS NOT NULL AND x BETWEEN DATE(prsr_start_dttm) AND DATE(prsr_end_dttm)
       QUALIFY ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY pr0_priority ASC, prd_dw_port_req_id, dw_subs_id) = 1 --NPP-19714
   )
   --NPP-19714<
   tt inner join
   (SELECT  dw_subs_id,prt0_port_req_type_cd,prd_port_req_completed_dttm
      FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_port_req_details_pstag`
     WHERE end_dttm IS NOT NULL AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
       AND pr_end_dttm IS NOT NULL AND x BETWEEN DATE(pr_start_dttm) AND DATE(pr_end_dttm)
       AND o_end_dttm IS NOT NULL AND x BETWEEN DATE(o_start_dttm) AND DATE(o_end_dttm)
       AND prt_end_dttm IS NOT NULL AND x BETWEEN DATE(prt_start_dttm) AND DATE(prt_end_dttm)
       AND prs_end_dttm IS NOT NULL AND x BETWEEN DATE(prs_start_dttm) AND DATE(prs_end_dttm)
       AND prsr_end_dttm IS NOT NULL AND x BETWEEN DATE(prsr_start_dttm) AND DATE(prsr_end_dttm)
       QUALIFY ROW_NUMBER() OVER (PARTITION BY dw_subs_id,prt0_port_req_type_cd ORDER BY pr0_priority ASC, prd_dw_port_req_id, dw_subs_id) = 1 --NPP-19714
   )ttt
   on tt.dw_subs_id = ttt.dw_subs_id
   GROUP BY dw_subs_id,
            dw_sub_subs_id,
            start_dttm,
            end_dttm,
            prd_dw_port_req_id,
            pr0_dw_port_req_stat_id,
            prd_port_req_completed_dttm,
            pr0_dw_donor_operator_id,
            pr0_dw_port_req_stat_rsn_id,
            pr0_dw_receiving_operator_id,
            prs0_dw_uniq_port_req_stat_id,
            prsr0_dw_uniq_port_req_stat_rsn_id,
            o0_dw_uniq_operator_id,
            prs0_port_req_stat_lvl_1_cd,
            prsr0_port_req_stat_rsn_lvl_1_cd,
            prt0_port_req_type_cd,
            pr0_priority,
            pr_start_dttm,
            pr_end_dttm,
            o_start_dttm,
            o_end_dttm,
            prt_start_dttm,
            prt_end_dttm,
            prs_start_dttm,
            prs_end_dttm,
            prsr_start_dttm,
            prsr_end_dttm
            -->NPP-19714
   ) prd0
   --### Start
  ON
    s0.dw_subs_id =prd0.dw_subs_id
    AND s0.dw_sub_subs_id=prd0.dw_sub_subs_id
    /*###AND s0.calendar_day_dt = prd0.calendar_day_dt*/

/*
Join R Subscriber Address with D Subscriber on subs id
Table used:
1- R Subscriber Address
2- D Geography
*/

  LEFT JOIN (
    SELECT
       g0.dw_geo_id AS g0_dw_geo_id,
      rsa.dw_subs_id,
      rsa.dw_sub_subs_id,
      g0.dw_uniq_geo_id AS g0_dw_uniq_geo_id,
      rsa.start_dttm, --###
      rsa.end_dttm --###
      /*###rsa.calendar_day_dt*/
    FROM (
            SELECT
             dw_geo_id,
            dw_subs_id,
            dw_sub_subs_id,
            start_dttm, --###
            end_dttm --###
            /*###calendar_day_dt*/
            FROM
            `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.r_subscriber_address`
            /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm*/
            WHERE
            end_dttm IS NOT NULL
            AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm) --###
            AND UPPER(cust_prim_addr_city) IN UNNEST(v_main_address)
            --AND x >= DATE(start_dttm)
            --AND x <= DATE(end_dttm)
        ) rsa
    INNER JOIN
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_geography` g0
    ON
      rsa.dw_geo_id=g0.dw_geo_id
      /*###AND timestamp(calendar_day_dt || ' 23:59:59') BETWEEN g0.start_dttm AND g0.end_dttm*/
    AND g0.end_dttm is not null
    AND x BETWEEN DATE(g0.start_dttm) AND DATE(g0.end_dttm) --###
      --AND x >= DATE(g0.start_dttm)
      --AND x <= DATE(g0.end_dttm)
    )rsa0
  ON
    s0.dw_subs_id =rsa0.dw_subs_id
    AND s0.dw_sub_subs_id = rsa0.dw_sub_subs_id
    /*###AND s0.calendar_day_dt = rsa0.calendar_day_dt*/

/* v12 ###
Join R Subscriber Contact Detail with D Subscriber on subs id
Table used:
1- R Subscriber Contact Detail
2- D Geography
*/

  LEFT JOIN (
    SELECT *
	  FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_sub_contact_detail_rn_pstag`
    ) rscd0
         ON s0.dw_subs_id = rscd0.dw_subs_id
        AND s0.dw_sub_subs_id = rscd0.dw_sub_subs_id

/*
Join R_CUSTOMER_ACCOUNT_ADDRESS with D Subscriber on cust account id
Table used:
1- R_CUSTOMER_ACCOUNT_ADDRESS
2- D Geography
*/

  LEFT JOIN (
    SELECT
       rcca.dw_cust_acct_id,
      rcca.dw_sub_cust_acct_id,
      g1.dw_geo_id AS g1_dw_geo_id,
      g1.dw_uniq_geo_id AS g1_dw_uniq_geo_id,
      rcca.start_dttm,
      rcca.end_dttm
      /*###rcca.calendar_day_dt*/
    FROM (
            SELECT
             dw_cust_acct_id,
            dw_sub_cust_acct_id,
            dw_geo_id,
            start_dttm, --###
            end_dttm --###
            /*###calendar_day_dt*/
            FROM
            `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.r_customer_account_address`
            /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm*/
            WHERE
            UPPER(addr_type) IN UNNEST(v_main_address)
            AND end_dttm IS NOT NULL
            AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm) --###
            --AND x >= DATE(start_dttm)
            --AND x <= DATE(end_dttm)
        )rcca
    INNER JOIN
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_geography`g1
    ON
      rcca.dw_geo_id=g1.dw_geo_id
      /*###AND timestamp(calendar_day_dt || ' 23:59:59') BETWEEN g1.start_dttm AND g1.end_dttm*/
      AND g1.end_dttm is not null
      AND x BETWEEN DATE(g1.start_dttm) AND DATE(g1.end_dttm) --###
      --AND x >= DATE(g1.start_dttm)
      --AND x <= DATE(g1.end_dttm)
    )rcca0
  ON
    s0.dw_cust_acct_id =rcca0.dw_cust_acct_id
    AND s0.dw_sub_cust_acct_id= rcca0.dw_sub_cust_acct_id
    /*###AND s0.calendar_day_dt = rcca0.calendar_day_dt*/

/* v12 ###
Join R Customer Account Contact Detail with D Subscriber on cust account id
Table used:
1- R_CUSTOMER_ACCOUNT_CONTACT_DETAIL
2- D Geography
*/

  LEFT JOIN (SELECT *
               FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_cust_acct_contact_detail_rn_pstag`
            )rcacd0
	     ON s0.dw_cust_acct_id = rcacd0.dw_cust_acct_id
        AND s0.dw_sub_cust_acct_id = rcacd0.dw_sub_cust_acct_id

/*
Join F Activity Base Semantic D to D Subscriber on subscriber id and date

Table used:
1- F Activity Base Semantic D
Pick the 1st record, i.e. the latest one or the latest available
This ensures selecting the latest available records when multiple are available.
Data must never be filtered out via this join and records must not be duplicated.

*/

  LEFT JOIN -- workaround as left join , originally it is inner join
      /*###`vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_activitybasesem_pstag` abs0*/
      --### Start
      (SELECT  rpt_dt
              ,dw_subs_id
              ,MAX(last_chargeable_event_dt) AS last_chargeable_event_dt --v12
         FROM (SELECT  * EXCEPT (rnk)
                 FROM (SELECT  rpt_dt
                              ,dw_subs_id
                              ,last_chargeable_event_dt
                              ,RANK() OVER(PARTITION BY dw_subs_id  ORDER BY rpt_dt DESC, dw_uniq_subs_id) rnk
                         FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_activitybasesem_pstag`
                        WHERE rpt_dt IS NOT null
                          AND rpt_dt BETWEEN DATE_SUB(x, INTERVAL 7 DAY) AND x
                      )
                WHERE rnk = 1
              )
      GROUP BY  rpt_dt
               ,dw_subs_id
      ) abs0
      --### End
  ON
    s0.dw_subs_id =abs0.dw_subs_id
    --v12 AND s0.dw_sub_subs_id = abs0.dw_sub_subs_id
    /*###AND s0.calendar_day_dt = abs0.calendar_day_dt*/

/*
Join D Device Association to D Subscriber on subscriber id and date
Step 1: Filter on date.
Step 2: Pick the device flagged in the EIM as being used for the main SIM.
Step 3 : If no main devide specified then pick the latest associated device.

Table used:
1-  D Device Association
2- D Device
3- D Device Category Association
4- D Device Category
*/

  LEFT JOIN (
    SELECT
       dca0.dw_dvc_cat_id AS dca0_dw_dvc_cat_id,
      dc0_dw_uniq_dvc_cat_id,
      da.dw_subs_id,
      da.dw_sub_subs_id,
      da.is_dvc_with_main_sim,
      da.dw_dvc_id,
      d0.dw_uniq_dvc_id AS d0_dw_uniq_dvc_id,
      da.start_dttm, --###
      da.end_dttm --###
      /*###da.calendar_day_dt*/
    FROM  /*###`vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_deviceassoc_pstag` da*/
         --### Start
         (SELECT * EXCEPT(dvc_with_main_sim_flg)
            FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_deviceassoc_rn_pstag`
           WHERE dvc_with_main_sim_flg =1) da
         --###
        LEFT JOIN
        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_device` d0
        ON
        da.dw_dvc_id= d0.dw_dvc_id
        /*###and timestamp(calendar_day_dt || ' 23:59:59') BETWEEN d0.start_dttm AND d0.end_dttm*/
        AND d0.end_dttm is not null
        AND x BETWEEN DATE(d0.start_dttm) AND DATE(d0.end_dttm) --###
        --AND x >= DATE(d0.start_dttm)
        --AND x <= DATE(d0.end_dttm)
        LEFT JOIN (
                SELECT
                     dca.dw_dvc_id,
                    dca.dw_dvc_cat_id,
                    dc0.dw_uniq_dvc_cat_id AS dc0_dw_uniq_dvc_cat_id,
                    dca.start_dttm, --###
                    dca.end_dttm --###
                    /*###dca.calendar_day_dt*/
                FROM (
                        SELECT
                        * except (rn)
                        /*###from `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_dvccatassoc_pstag`*/
                          FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_dvccatassoc_rn_pstag` --###
                        where rn = 1
                          AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm) --###
                    ) dca
                LEFT JOIN
                    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_device_category` dc0
                ON
                    dc0.dw_dvc_cat_id= dca.dw_dvc_cat_id
                    /*###and timestamp(calendar_day_dt || ' 23:59:59') BETWEEN dc0.start_dttm AND dc0.end_dttm*/
                    AND dc0.end_dttm is not null
                    AND x BETWEEN DATE(dc0.start_dttm) AND DATE(dc0.end_dttm) --###
                    --AND x >= DATE(dc0.start_dttm)
                    --AND x <= DATE(dc0.end_dttm)
            )dca0
        ON
        da.dw_dvc_id= dca0.dw_dvc_id
        /*###AND da.calendar_day_dt = dca0.calendar_day_dt*/
    )da0
  ON
    s0.dw_subs_id =da0.dw_subs_id
    AND s0.dw_sub_subs_id = da0.dw_sub_subs_id
    /*###AND s0.calendar_day_dt = da0.calendar_day_dt*/

/*
This join is to get segment info joining with d_subscriber on dw subs id
Step 1: Filter on date.
Step 2: Filter on the subscriber segment used for group reporting.
*/

  LEFT JOIN ( -- workaround as left join , originally it is inner join
    SELECT
       ssa.dw_subs_id,
      ssa.dw_sub_subs_id,
      ssa.dw_subs_seg_id,
      sseg0.dw_uniq_subs_seg_id AS sseg0_dw_uniq_subs_seg_id,
      sseg0.subs_seg_lvl_1_cd AS sseg0_subs_seg_lvl_1_cd/*###,*/
      /*###calendar_day_dt*/
    /*###FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_subssegassoc_pstag` ssa*/
    FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_subssegassoc_rn_pstag` ssa --###
    INNER JOIN
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_segment` sseg0
    ON
      ssa.dw_subs_seg_id=sseg0.dw_subs_seg_id
      /*###and timestamp(calendar_day_dt || ' 23:59:59') BETWEEN sseg0.start_dttm AND sseg0.end_dttm*/
      AND sseg0.end_dttm is not null
      AND x BETWEEN DATE(sseg0.start_dttm) AND DATE(sseg0.end_dttm)
      --AND x >= DATE(sseg0.start_dttm)
      --AND x <= DATE(sseg0.end_dttm)
      WHERE rn = 1
    )ssa0

  ON
    s0.dw_subs_id = ssa0.dw_subs_id
    AND s0.dw_sub_subs_id = ssa0.dw_sub_subs_id
    /*###AND s0.calendar_day_dt = ssa0.calendar_day_dt*/

/*
Joining with D Tariff Plan, D Subscriber Status Reason, D Subscriber Status, D Subscriber Barring Status.
*/

  INNER JOIN
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_tariff_plan` tp0
  ON
    s0.dw_rpt_tariff_plan_id =tp0.dw_tariff_plan_id
    /*###and timestamp(s0.calendar_day_dt || ' 23:59:59') BETWEEN tp0.start_dttm AND tp0.end_dttm*/
    AND tp0.end_dttm is not null
    AND x BETWEEN DATE(tp0.start_dttm) AND DATE(tp0.end_dttm) --###
    --AND x >= DATE(tp0.start_dttm)
    --AND x <= DATE(tp0.end_dttm)
  INNER JOIN
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_status_reason` ssr0
  ON
    s0.dw_subs_stat_rsn_id = ssr0.dw_subs_stat_rsn_id
    /*###and timestamp(s0.calendar_day_dt || ' 23:59:59') BETWEEN ssr0.start_dttm AND ssr0.end_dttm*/
     AND ssr0.end_dttm is not null
     AND x BETWEEN DATE(ssr0.start_dttm) AND DATE(ssr0.end_dttm) --###
    --AND x >= DATE(ssr0.start_dttm)
    --AND x <= DATE(ssr0.end_dttm)
  INNER JOIN
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_status` ss0
  ON
    s0.dw_subs_stat_id = ss0.dw_subs_stat_id
    /*###and timestamp(s0.calendar_day_dt || ' 23:59:59') BETWEEN ss0.start_dttm AND ss0.end_dttm*/
    AND ss0.end_dttm is not null
    AND x BETWEEN DATE(ss0.start_dttm) AND DATE(ss0.end_dttm)
    --AND x >= DATE(ss0.start_dttm)
    --AND x <= DATE(ss0.end_dttm)
  LEFT JOIN -- workaround as left join, originally it is inner join
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_barring_status` sbs0
  ON
    s0.dw_subs_barring_stat_id =sbs0.dw_subs_barring_stat_id
    /*###and timestamp(s0.calendar_day_dt || ' 23:59:59') BETWEEN sbs0.start_dttm AND sbs0.end_dttm*/
  and sbs0.end_dttm is not null
  AND x BETWEEN DATE(sbs0.start_dttm) AND DATE(sbs0.end_dttm) --###
    --AND x >= DATE(sbs0.start_dttm)
    --AND x <= DATE(sbs0.end_dttm)

/*
This join is to get sales force position ang sales force type ids by joining d_sales_force_position table with d_subscriber
Tables used:
1- d_sales_force_position
2- d_sales_force_type
*/

  LEFT JOIN ( -- workaround as left join due to sales force potential CR, originally it is inner join
    SELECT
       sfp.dw_sf_position_id,
      sfp.dw_sf_type_id,
      sft0.dw_sf_type_id AS sft0_dw_sf_type_id,
      /*###sfp.calendar_day_dt*/
      sft0.start_dttm, --###
      sft0.end_dttm --###
    FROM (
            SELECT
                 dw_sf_position_id,
                dw_sf_type_id,
                /*###calendar_day_dt*/
                start_dttm, --###
                end_dttm --###
            FROM
                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_sales_force_position`
                /*###inner join ( select calendar_day_dt from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_calendar`
                where calendar_day_dt between x and y) cal
                ON
                 timestamp(calendar_day_dt || ' 23:59:59') BETWEEN start_dttm AND end_dttm AND end_dttm is not null*/
            --WHERE
            --  x >= DATE(start_dttm)
            --  AND x <= DATE(end_dttm)
            WHERE end_dttm IS NOT NULL AND start_dttm <= timestamp(y || ' 23:59:59') AND end_dttm >= timestamp(x || ' 00:00:00') --###
        ) sfp
    INNER JOIN
      `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_sales_force_type` sft0
    ON
      sft0.dw_sf_type_id=sfp.dw_sf_type_id
      /*###AND timestamp(calendar_day_dt || ' 23:59:59') BETWEEN sft0.start_dttm AND sft0.end_dttm AND sft0.end_dttm is not null*/
      --AND x >= DATE(sft0.start_dttm)
      --AND x <= DATE(sft0.end_dttm)
      AND sft0.end_dttm IS NOT NULL AND sft0.start_dttm <= timestamp(y || ' 23:59:59') AND sft0.end_dttm >= timestamp(x || ' 00:00:00') --###
      ) sfp0
  ON
    s0.dw_sf_position_id =sfp0.dw_sf_position_id
    /*###AND s0.calendar_day_dt = sfp0.calendar_day_dt*/

/*
This join is to get customet details along with Convergence details.

Tables used:
1- d_customer_account         7- d_customer_convergence_type
2- d_customer                 8- d_convergence_type
3- d_customer_segment         9- r_customer_address
4- d_customer_category        10- d_geography
5- d_customer_type            11- d_customer_account_category
6- d_sector
*/
  INNER JOIN (
    SELECT * --###
      FROM ( --###
SELECT * FROM
     /*###`vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_cust_acct_pstag`*/
     `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_cust_acct_rnk_pstag` --###
where rnk=1
--### Start
           ) ca
             LEFT JOIN (SELECT  rca.dw_cust_id as rca_dw_cust_id
                               ,rca.dw_sub_cust_id as rca_dw_sub_cust_id
                               ,g2.dw_geo_id AS g2_dw_geo_id
                               ,g2.dw_uniq_geo_id AS g2_dw_uniq_geo_id
                          FROM (SELECT * EXCEPT (rank)
                                  FROM (SELECT  dw_cust_id
                                               ,dw_sub_cust_id
                                               ,dw_geo_id
                                               ,RANK() OVER( PARTITION BY dw_cust_id ORDER BY start_dttm DESC, end_dttm DESC, dw_cust_addr_id desc) rank
                                          FROM `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.r_customer_address`
                                         WHERE UPPER(addr_type) IN UNNEST(v_main_address)
                                           AND end_dttm IS NOT NULL AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
                                       )
                                 WHERE rank = 1
                               ) rca
                                 INNER JOIN `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_geography` g2
                                         ON rca.dw_geo_id = g2.dw_geo_id
                                        AND g2.end_dttm IS NOT NULL AND x BETWEEN DATE(g2.start_dttm) AND DATE(g2.end_dttm)
                       )rca0
                    ON rca0.rca_dw_cust_id = ca.dw_cust_id
                   AND rca0.rca_dw_sub_cust_id = ca.dw_sub_cust_id
             LEFT JOIN (SELECT  *
			              FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_cust_contact_detail_rn_pstag`
					   )rccd0
                    ON rccd0.rccd_dw_cust_id = ca.dw_cust_id
                   AND rccd0.rccd_dw_sub_cust_id = ca.dw_sub_cust_id
     WHERE end_dttm IS NOT NULL AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm)
       AND end_dttm IS NOT NULL AND x BETWEEN DATE(c_start_dttm) AND DATE(c_end_dttm)
       AND end_dttm IS NOT NULL AND x BETWEEN DATE(cs_start_dttm) AND DATE(cs_end_dttm)
       AND end_dttm IS NOT NULL AND x BETWEEN DATE(cc_start_dttm) AND DATE(cc_end_dttm)
       AND end_dttm IS NOT NULL AND x BETWEEN DATE(ct_start_dttm) AND DATE(ct_end_dttm)
       AND end_dttm IS NOT NULL AND x BETWEEN DATE(sec_start_dttm) AND DATE(sec_end_dttm)
       AND end_dttm IS NOT NULL AND x BETWEEN DATE(cac_start_dttm) AND DATE(cac_end_dttm)
--### End

)ca0
  ON
    s0.dw_cust_acct_id= ca0.dw_cust_acct_id
  --  AND s0.dw_sub_cust_acct_id = ca0.dw_sub_cust_acct_id
  /*###AND s0.calendar_day_dt = ca0.calendar_day_dt*/

/*
Join to get physical Line &  Fixed Line details.
Tables used:
1- D Physical Line
2- D Fixed Line Type
*/

  LEFT JOIN (
            SELECT
             pl.dw_physical_line_id,
            pl.dw_uniq_physical_line_id,
            pl.dw_fixed_line_type_id,
            flt0.dw_uniq_fixed_line_type_id,
            pl.start_dttm, --###
            pl.end_dttm --###
            /*calendar_day_dt*/
            /*###FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_physicalline_pstag` pl*/
            FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_physicalline_rn_pstag` pl
            LEFT JOIN
            `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_fixed_line_type` flt0
            ON
            pl.dw_fixed_line_type_id = flt0.dw_fixed_line_type_id
            /*###AND timestamp(calendar_day_dt || ' 23:59:59') BETWEEN flt0.start_dttm AND flt0.end_dttm AND flt0.end_dttm is not null*/
            --AND x >= DATE(flt0.start_dttm)
            --AND x <= DATE(flt0.end_dttm)
            AND flt0.end_dttm IS NOT NULL AND x BETWEEN DATE(flt0.start_dttm) AND DATE(flt0.end_dttm) --###
            WHERE pl_rn = 1
            ) pl0
  ON
    s0.dw_physical_line_id = pl0.dw_physical_line_id
    /*###and s0.calendar_day_dt = pl0.calendar_day_dt*/
    );



--------------END: FINAL BASE ALL DIMENSION WITH AGREEMENT RELATED COLUMNS--------
-----************************END**************************--------
----------BASE ALL DIMENSION STAGING TABLE CREATION --------------
-----************************END**************************--------

-----************************BEGIN**************************--------
----------GROSS ADD RECORDS STAGING TABLE CREATION --------------
-----************************BEGIN**************************--------

    CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_gross_add_pstag` PARTITION BY rpt_dt AS --###
SELECT
  target0.*,
  to_hex(sha256("*NA")) AS dw_prev_business_line_id,
  CAST(NULL AS date) AS last_business_line_change_dt,
  to_hex(sha256("*NA")) AS dw_prev_rpt_tariff_plan_id,
  CAST(NULL AS date) AS last_rpt_tariff_plan_change_dt,
  to_hex(sha256("*NA")) AS dw_prev_subs_seg_id,
  CAST(NULL AS date) AS last_subs_seg_change_dt,
  CAST(NULL AS date) AS subs_last_retention_dt,
  0 AS business_line_change_ind,
  0 AS tariff_plan_change_ind,
  0 AS subs_seg_change_ind,
  to_hex(sha256("*NA")) AS dw_last_retention_sf_position_id,
  to_hex(sha256("*NA")) AS dw_last_retention_sf_type_id,
  to_hex(sha256("*NA")) AS dw_prev_fixed_line_type_id,
  CAST(NULL AS date) AS last_fixed_line_type_change_dt,
  subs_conn_rpt_dt AS inactive_to_active_change_dt,
  0 AS retention_ind,
  -- v9 changes begin
  to_hex(sha256("*NA")) AS dw_prev_primary_dvc_id,
  dw_curr_business_line_id AS dw_acq_business_line_id,
  dw_curr_subs_seg_id AS dw_acq_subs_seg_id,
  dw_curr_rpt_tariff_plan_id AS dw_acq_rpt_tariff_plan_id,
  to_hex(sha256("*NA")) AS dw_dconn_business_line_id,
  to_hex(sha256("*NA")) AS dw_dconn_subs_seg_id,
  to_hex(sha256("*NA")) AS dw_dconn_rpt_tariff_plan_id,
  to_hex(sha256("*NA")) AS dw_dconn_stat_rsn_id,
  CAST(NULL AS date) AS last_primary_dvc_change_dt,
  0 as primary_dvc_change_ind
  --- v9 changes end
FROM (
  SELECT
    business_line_lvl_2_cd,
    subs_seg_lvl_1_cd,
    dconn_ind,
    dw_curr_business_line_id,
    dw_curr_rpt_tariff_plan_id,
    dw_curr_subs_seg_id,
    dw_cust_acct_geo_id,
    dw_cust_acct_id,
    dw_cust_geo_id,
    dw_cust_id,
    dw_cust_sector_id,
    dw_cust_seg_id,
    dw_cust_type_id,
    dw_physical_line_id,
    dw_port_in_operator_id,
    dw_port_out_operator_id,
    dw_port_req_stat_id,
    dw_port_req_stat_rsn_id,
    dw_curr_primary_dvc_cat_id,
    dw_curr_primary_dvc_id,
    dw_subs_barring_stat_id,
    dw_subs_geo_id,
    dw_subs_id,
    dw_subs_rpt_stat_id,
    dw_subs_stat_id,
    dw_subs_stat_rsn_id,
    dw_uniq_current_business_line_id,
    dw_uniq_current_rpt_tariff_plan_id,
    dw_uniq_current_subs_seg_id,
    dw_uniq_cust_acct_geo_id,
    dw_uniq_cust_acct_id,
    dw_uniq_cust_geo_id,
    dw_uniq_cust_id,
    dw_uniq_cust_sector_id,
    dw_uniq_cust_seg_id,
    dw_uniq_cust_type_id,
    dw_uniq_port_out_operator_id,
    dw_uniq_port_req_stat_id,
    dw_uniq_port_req_stat_rsn_id,
    dw_uniq_curr_primary_dvc_cat_id,
    dw_uniq_curr_primary_dvc_id,
    dw_uniq_subs_barring_stat_id,
    dw_uniq_subs_geo_id,
    dw_uniq_subs_id,
    dw_uniq_subs_rpt_stat_id,
    dw_uniq_subs_stat_id,
    dw_uniq_subs_stat_rsn_id,
    gross_add_ind,
    is_dummy_cust,
    is_in_active_base,
    is_in_closing_base,
    is_in_contract_commitment,
    is_port_in,
	port_in_completion_dt, --NPP-19714
	port_out_completion_dt, --NPP-19714
    is_port_out,
    last_chargeable_event_dt,
    migr_of_addr_ind,
    migr_of_tech_ind,
    reconnection_ind,
    rpt_dt,
    subs_barring_dt,
    subs_conn_num,
    subs_conn_rpt_dt,
    subs_dconn_rpt_dt,
    subs_preactivation_rpt_dt,
    subs_reconnection_rpt_dt,
    subs_rgstn_dt,
    subs_agmt_last_term_dt,
    with_commitment,
    dw_acq_sf_position_id,
    dw_acq_sf_type_id,
    dw_first_acquired_prod_id,
    dw_sub_first_acquired_prod_id,
    dw_uniq_physical_line_id,
    dw_curr_fixed_line_type_id,
    dw_uniq_curr_fixed_line_type_id,
    active_to_inactive_change_dt,
	ss_cd  --v12 changes
  FROM
    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_basealldims_pstag`
  WHERE
     gross_add_ind=1) target0;


-----************************END**************************--------
----------GROSS ADD RECORDS STAGING TABLE CREATION --------------
-----************************END**************************--------

-----************************BEGINN**************************--------
----------------RECORDS WITH GROSS ADDS NOT EQUAL TO 1---------------
----------------------HISTORY FROMM TARGET---------------------------
CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_history_from_tgt_pstag` PARTITION BY rpt_dt AS --###
SELECT
  business_line_lvl_2_cd,
  subs_seg_lvl_1_cd,
  dconn_ind,
  dw_curr_business_line_id,
  dw_curr_rpt_tariff_plan_id,
  dw_curr_subs_seg_id,
  dw_cust_acct_geo_id,
  dw_cust_acct_id,
  dw_cust_geo_id,
  dw_cust_id,
  dw_cust_sector_id,
  dw_cust_seg_id,
  dw_cust_type_id,
  dw_physical_line_id,
  dw_port_in_operator_id,
  dw_port_out_operator_id,
  dw_port_req_stat_id,
  dw_port_req_stat_rsn_id,
  dw_curr_primary_dvc_cat_id,
  dw_curr_primary_dvc_id,
  dw_subs_barring_stat_id,
  dw_subs_geo_id,
  dw_subs_id,
  dw_subs_rpt_stat_id,
  dw_subs_stat_id,
  dw_subs_stat_rsn_id,
  dw_uniq_current_business_line_id,
  dw_uniq_current_rpt_tariff_plan_id,
  dw_uniq_current_subs_seg_id,
  dw_uniq_cust_acct_geo_id,
  dw_uniq_cust_acct_id,
  dw_uniq_cust_geo_id,
  dw_uniq_cust_id,
  dw_uniq_cust_sector_id,
  dw_uniq_cust_seg_id,
  dw_uniq_cust_type_id,
  dw_uniq_port_out_operator_id,
  dw_uniq_port_req_stat_id,
  dw_uniq_port_req_stat_rsn_id,
  dw_uniq_curr_primary_dvc_cat_id,
  dw_uniq_curr_primary_dvc_id,
  dw_uniq_subs_barring_stat_id,
  dw_uniq_subs_geo_id,
  dw_uniq_subs_id,
  dw_uniq_subs_rpt_stat_id,
  dw_uniq_subs_stat_id,
  dw_uniq_subs_stat_rsn_id,
  gross_add_ind,
  is_dummy_cust,
  is_in_active_base,
  is_in_closing_base,
  is_in_contract_commitment,
  is_port_in,
  port_in_completion_dt, --NPP-19714
  port_out_completion_dt, --NPP-19714
  is_port_out,
  last_chargeable_event_dt,
  migr_of_addr_ind,
  migr_of_tech_ind,
  reconnection_ind,
  rpt_dt,
  subs_barring_dt,
  subs_conn_num,
  subs_conn_rpt_dt,
  subs_dconn_rpt_dt,
  subs_preactivation_rpt_dt,
  subs_reconnection_rpt_dt,
  subs_rgstn_dt,
  subs_agmt_last_term_dt,
  with_commitment,
  dw_last_retention_sf_position_id,
  dw_last_retention_sf_type_id,
  subs_last_retention_dt,
  business_line_change_ind,
  tariff_plan_change_ind,
  subs_seg_change_ind,
  dw_acq_sf_position_id,
  dw_acq_sf_type_id,
  dw_first_acquired_prod_id,
  dw_sub_first_acquired_prod_id,
  dw_prev_business_line_id,
  last_business_line_change_dt,
  dw_prev_rpt_tariff_plan_id,
  last_rpt_tariff_plan_change_dt,
  dw_prev_subs_seg_id,
  last_subs_seg_change_dt,
  dw_uniq_physical_line_id,
  dw_curr_fixed_line_type_id,
  dw_uniq_curr_fixed_line_type_id,
  dw_prev_fixed_line_type_id,
  last_fixed_line_type_change_dt,
  inactive_to_active_change_dt,
  active_to_inactive_change_dt,
  target2_converge_change_dt,
  target2_rgu_bundle_change_dt,
  target2_play_count_change_dt,
  target2_dw_cust_id,
  target2_rpt_dt,
  target2_play_count_change,
  target2_is_in_closing_base,
  retention_ind,
  chk,
  agreement_count,
  -- v9 change BEGIN --
  dw_prev_primary_dvc_id,
  dw_acq_business_line_id,
  dw_acq_subs_seg_id,
  dw_acq_rpt_tariff_plan_id,
  dw_dconn_business_line_id,
  dw_dconn_subs_seg_id,
  dw_dconn_rpt_tariff_plan_id,
  dw_dconn_stat_rsn_id,
  last_primary_dvc_change_dt,
  primary_dvc_change_ind,
  -- v9 change end --
  ss_cd --v12 changes
FROM (
  SELECT
    *,
    CASE
      WHEN retention_ind=1 THEN t1_dw_last_retention_sf_position_id
      ELSE t2_dw_last_retention_sf_position_id
    END AS dw_last_retention_sf_position_id,
    CASE
      WHEN retention_ind=1 THEN t1_dw_last_retention_sf_type_id
      ELSE t2_dw_last_retention_sf_type_id
    END AS dw_last_retention_sf_type_id,
    CASE
      WHEN retention_ind=1 THEN rpt_dt
      ELSE t2_subs_last_retention_dt
    END AS subs_last_retention_dt,
    CASE
      WHEN last_business_line_change_dt= rpt_dt THEN 1
      ELSE 0
    END AS business_line_change_ind,
    CASE
      WHEN last_rpt_tariff_plan_change_dt = rpt_dt THEN 1
      ELSE 0
    END AS tariff_plan_change_ind,
    CASE
      WHEN last_subs_seg_change_dt= rpt_dt THEN 1
      ELSE 0
    END AS subs_seg_change_ind,
    -- v9 changes begin
    CASE
      WHEN last_primary_dvc_change_dt = rpt_dt THEN 1
      ELSE 0
    END AS primary_dvc_change_ind
    -- v9 changes end
  FROM (
    SELECT
      target1.business_line_lvl_2_cd,
      target1.subs_seg_lvl_1_cd,
      target1.dconn_ind,
      target1.dw_curr_business_line_id,
      target1.dw_curr_rpt_tariff_plan_id,
      target1.dw_curr_subs_seg_id,
      target1.dw_cust_acct_geo_id,
      target1.dw_cust_acct_id,
      target1.dw_cust_geo_id,
      target1.dw_cust_id,
      target1.dw_cust_sector_id,
      target1.dw_cust_seg_id,
      target1.dw_cust_type_id,
      target1.dw_physical_line_id,
      target1.dw_port_out_operator_id,
      target1.dw_port_req_stat_id,
      target1.dw_port_req_stat_rsn_id,
      target1.dw_curr_primary_dvc_cat_id,
      target1.dw_curr_primary_dvc_id,
      target1.dw_subs_barring_stat_id,
      target1.dw_subs_geo_id,
      target1.dw_subs_id,
      target1.dw_subs_rpt_stat_id,
      target1.dw_subs_stat_id,
      target1.dw_subs_stat_rsn_id,
      target1.dw_uniq_current_business_line_id,
      target1.dw_uniq_current_rpt_tariff_plan_id,
      target1.dw_uniq_current_subs_seg_id,
      target1.dw_uniq_cust_acct_geo_id,
      target1.dw_uniq_cust_acct_id,
      target1.dw_uniq_cust_geo_id,
      target1.dw_uniq_cust_id,
      target1.dw_uniq_cust_sector_id,
      target1.dw_uniq_cust_seg_id,
      target1.dw_uniq_cust_type_id,
      target1.dw_uniq_port_out_operator_id,
      target1.dw_uniq_port_req_stat_id,
      target1.dw_uniq_port_req_stat_rsn_id,
      target1.dw_uniq_curr_primary_dvc_cat_id,
      target1.dw_uniq_curr_primary_dvc_id,
      target1.dw_uniq_subs_barring_stat_id,
      target1.dw_uniq_subs_geo_id,
      target1.dw_uniq_subs_id,
      target1.dw_uniq_subs_rpt_stat_id,
      target1.dw_uniq_subs_stat_id,
      target1.dw_uniq_subs_stat_rsn_id,
      target1.gross_add_ind,
      target1.is_dummy_cust,
      target1.is_in_active_base,
      target1.is_in_closing_base,
      target1.is_in_contract_commitment,
      target1.is_port_out,
      target1.last_chargeable_event_dt,
      target1.migr_of_addr_ind,
      target1.migr_of_tech_ind,
      target1.reconnection_ind,
      target1.rpt_dt,
      target1.subs_barring_dt,
      target1.subs_conn_num,
      target1.subs_conn_rpt_dt,
      target1.subs_dconn_rpt_dt,
      target1.subs_preactivation_rpt_dt,
      target1.subs_reconnection_rpt_dt,
      target1.subs_rgstn_dt,
      target1.subs_agmt_last_term_dt,
      target1.with_commitment,
	  target1.ss_cd, --v12 changes
      target2.chk,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL
        AND ifnull(target1.dw_curr_business_line_id,to_hex(sha256("*NA")))= ifnull(target2.dw_curr_business_line_id,to_hex(sha256("*NA"))) THEN target2.dw_prev_business_line_id
        ELSE target2.dw_curr_business_line_id
      END  AS dw_prev_business_line_id,
      CASE
        WHEN target1.retention_ind IS NULL and target1.agreement_count > 0 --and target1.subs_agmt_last_term_dt > target2.subs_agmt_last_term_dt
        THEN 1
        ELSE 0
      END AS retention_ind,
      CASE
        WHEN ifnull(target1.dw_curr_business_line_id,to_hex(sha256("*NA")))= ifnull(target2.dw_curr_business_line_id,to_hex(sha256("*NA"))) THEN target2.last_business_line_change_dt
        ELSE target1.rpt_dt
      END AS last_business_line_change_dt,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL
        AND ifnull(target1.dw_curr_rpt_tariff_plan_id,to_hex(sha256("*NA"))) =ifnull(target2.dw_curr_rpt_tariff_plan_id,to_hex(sha256("*NA"))) THEN target2.dw_prev_rpt_tariff_plan_id
        ELSE target2.dw_curr_rpt_tariff_plan_id
      END AS dw_prev_rpt_tariff_plan_id,
      CASE
        WHEN ifnull(target1.dw_curr_rpt_tariff_plan_id,to_hex(sha256("*NA"))) = ifnull(target2.dw_curr_rpt_tariff_plan_id,to_hex(sha256("*NA"))) THEN target2.last_rpt_tariff_plan_change_dt
        ELSE target1.rpt_dt
      END AS last_rpt_tariff_plan_change_dt,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL
        AND ifnull(target1.dw_curr_subs_seg_id,to_hex(sha256("*NA"))) =ifnull(target2.dw_curr_subs_seg_id,to_hex(sha256("*NA"))) THEN target2.dw_prev_subs_seg_id
        ELSE target2.dw_curr_subs_seg_id
      END AS dw_prev_subs_seg_id,
      CASE
        WHEN ifnull(target1.dw_curr_subs_seg_id,to_hex(sha256("*NA"))) =ifnull(target2.dw_curr_subs_seg_id,to_hex(sha256("*NA"))) THEN target2.last_subs_seg_change_dt
        ELSE target1.rpt_dt
      END AS last_subs_seg_change_dt,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL THEN target2.dw_acq_sf_position_id
        ELSE NULL
      END AS dw_acq_sf_position_id,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL THEN target2.dw_acq_sf_type_id
        ELSE NULL
      END AS dw_acq_sf_type_id,
      target1.dw_last_retention_sf_position_id AS t1_dw_last_retention_sf_position_id,
      target2.dw_last_retention_sf_position_id AS t2_dw_last_retention_sf_position_id,
      target1.dw_last_retention_sf_type_id AS t1_dw_last_retention_sf_type_id,
      target2.dw_last_retention_sf_type_id AS t2_dw_last_retention_sf_type_id,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL THEN target2.dw_first_acquired_prod_id
        ELSE NULL
      END AS dw_first_acquired_prod_id,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL THEN target2.dw_sub_first_acquired_prod_id
        ELSE NULL
      END AS dw_sub_first_acquired_prod_id,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL THEN target2.is_port_in
        ELSE NULL
      END AS is_port_in,
	  IFNULL(target1.port_in_completion_dt,target2.port_in_completion_dt) AS port_in_completion_dt, --NPP-19714
	  IFNULL(target1.port_out_completion_dt,target2.port_out_completion_dt) AS port_out_completion_dt, --NPP-19714
      CASE
        WHEN target2.dw_subs_id IS NOT NULL THEN target2.dw_port_in_operator_id
        ELSE NULL
      END AS dw_port_in_operator_id,
      target2.subs_last_retention_dt AS t2_subs_last_retention_dt,
      target1.dw_uniq_physical_line_id,
      target1.dw_curr_fixed_line_type_id,
      target1.dw_uniq_curr_fixed_line_type_id,
      CASE
        WHEN target2.dw_subs_id IS NOT NULL
        AND ifnull(target1.dw_curr_fixed_line_type_id,to_hex(sha256("*NA"))) =ifnull(target2.dw_curr_fixed_line_type_id,to_hex(sha256("*NA"))) THEN target2.dw_prev_fixed_line_type_id
        ELSE target2.dw_curr_fixed_line_type_id
      END AS dw_prev_fixed_line_type_id,
      CASE
        WHEN ifnull(target1.dw_curr_fixed_line_type_id,to_hex(sha256("*NA"))) =ifnull(target2.dw_curr_fixed_line_type_id,to_hex(sha256("*NA"))) THEN target2.last_fixed_line_type_change_dt
        ELSE target1.rpt_dt
      END AS last_fixed_line_type_change_dt,
      target2.converge_change_dt as target2_converge_change_dt,
      target2.rgu_bundle_change_dt as target2_rgu_bundle_change_dt,
      target2.play_count_change_dt as target2_play_count_change_dt,
      target2.dw_cust_id as target2_dw_cust_id,
      target2.rpt_dt as target2_rpt_dt,
      target2.play_count_change as target2_play_count_change,
      target2.is_in_closing_base as target2_is_in_closing_base,
      CASE
        WHEN target1.is_in_active_base = target2.is_in_active_base THEN target2.inactive_to_active_change_dt
        WHEN target1.is_in_active_base = 1
        AND target2.is_in_active_base = 0 THEN target1.rpt_dt
        ELSE NULL
      END AS inactive_to_active_change_dt,
      target1.active_to_inactive_change_dt,
      target1.agreement_count,
      -- v9 changes begin
      CASE
        WHEN target2.dw_subs_id IS NOT NULL
        AND ifnull(target1.dw_curr_primary_dvc_id,to_hex(sha256("*NA"))) =ifnull(target2.dw_curr_prim_dvc_id,to_hex(sha256("*NA"))) THEN target2.dw_prev_prim_dvc_id
        ELSE target2.dw_curr_prim_dvc_id
      END AS dw_prev_primary_dvc_id,
      target2.dw_acq_business_line_id,
      target2.dw_acq_rpt_tariff_plan_id,
      target2.dw_acq_subs_seg_id,
      CASE
        WHEN target1.dconn_ind = 1 THEN target1.dw_curr_business_line_id
        ELSE target2.dw_dconn_business_line_id
      END AS dw_dconn_business_line_id,
      CASE
        WHEN target1.dconn_ind = 1 THEN target1.dw_curr_subs_seg_id
        ELSE target2.dw_dconn_subs_seg_id
      END AS dw_dconn_subs_seg_id,
      CASE
        WHEN target1.dconn_ind = 1 THEN target1.dw_curr_rpt_tariff_plan_id
        ELSE target2.dw_dconn_rpt_tariff_plan_id
      END AS dw_dconn_rpt_tariff_plan_id,
       CASE
        WHEN target1.dconn_ind = 1 THEN target1.dw_subs_stat_rsn_id
        ELSE target2.dw_dconn_stat_rsn_id
      END AS dw_dconn_stat_rsn_id,
       CASE
        WHEN ifnull(target1.dw_curr_primary_dvc_id,to_hex(sha256("*NA"))) = ifnull(target2.dw_curr_prim_dvc_id,to_hex(sha256("*NA"))) THEN target2.last_prim_dvc_change_dt
        ELSE target1.rpt_dt
      END AS last_primary_dvc_change_dt,
      -- v9 changes end
    FROM
        (
            SELECT
                *
            FROM
                `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_basealldims_pstag`
            WHERE
                rpt_dt = x
                AND gross_add_ind<>1
        ) target1
    LEFT JOIN
        (
            SELECT
                * EXCEPT(rn),
                1 AS chk
            FROM (
                    SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY rpt_dt DESC, dw_curr_business_line_id, dw_curr_rpt_tariff_plan_id  ) rn -- row_number handling
                    FROM
                    /*###`vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_subscriber_base_semantic_d`  */
                    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_hist_subs_base_sem_d_and_m_pstag`
                    where rpt_dt < x
                )
            WHERE
                rn=1
        ) target2
    ON
      target1.dw_subs_id=target2.dw_subs_id
    )
    ) ;

----------------RECORDS WITH GROSS ADDS NOT EQUAL TO 1-------------
----------------------HISTORY FROMM TARGET-------------------------
-----************************ENDD**************************--------

-----************************BEGINN**************************--------
----------------RECORDS WITH GROSS ADDS NOT EQUAL TO 1---------------
-----------------------HISTORY FROMM SOURCE SCENARIO-----------------


/*
This scenario is valid for  first run only i.e if the target f_subscriber_base_semantic_d table is not having processing day-2 days data
if processing day-2 (x-1) days records are present in Target table then this block should be skipped.
To implement this following logic is being applied:

Step 1: Check if the x-1 rpt_dt records are present in target
Step 2: If No:  Then execute History from Source Scenario, create or replace f_subsbasesemd_history_from_src_stag table
        If Yes: Then Truncate f_subsbasesemd_history_from_src_stag table.

    This truncate is done so that Union All Block is not impacted.

*/

IF EXISTS (SELECT 1 FROM `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_subscriber_base_semantic_d`  WHERE rpt_dt = x-1)
THEN
    TRUNCATE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_history_from_src_pstag`;
    SET is_data_in_tgt = 1;

ELSE

/*Proceed with History from Source Scenario Execution */

/*
START: TEMP D_SUBSCRIBER TABLE CREATION STARTS
This step is creating a temp  f_subsbasesemd_histfrmsrc_dsubs_stag table which is formed joining
1- d_subscriber
2- d_subscriber_reporting_status
3- d_business_line

This table is created to avoid hitting multiple times d_subscriber table, also to reduce repetetive code.
In History From Source Scenario, this table will be used wherever the above mentioned table joins are required.

There are two main condition added to reduce the data volume in this table
    a) (d_subscriber.start_dttm) < x  – This will pick only the records which are previous to rpt_dt

    b) And pick only subs which are available in f_subsbasesemd_basealldims_stag. (inner join to f_subsbasesemd_basealldims_stag)

*/
SET is_data_in_tgt = 0;  -- need to ignore records which are covered in history from tgt scenario.

        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag` AS
        SELECT
        s.start_dttm,
        s.dw_business_line_id,
        s.end_dttm,
        s.dw_subs_id,
        srs.subs_rpt_stat_cd,
        s.dw_rpt_tariff_plan_id,
        s.dw_sub_subs_id,
        s.subs_conn_rpt_dt,
        s.dw_prod_id,
        s.dw_sub_prod_id,
        s.dw_sf_position_id,
        bl.business_line_lvl_2_cd,
        bl.business_line_lvl_3_cd,
        s.subs_dconn_rpt_dt,
        bl.start_dttm AS s0,
        bl.end_dttm AS e0,
        srs.start_dttm AS s1,
        srs.end_dttm AS e1,
        bl.business_line_lvl_1_cd AS bl_business_line_lvl_1_cd,
        s.dw_physical_line_id,
        ssax.subs_seg_ss_type,
        s.dw_subs_stat_rsn_id,
        ssax.dw_subs_seg_id
        FROM
        (
            SELECT
                    dw_subs_id,
                   dw_business_line_id,
                   end_dttm,
                   start_dttm,
                   dw_rpt_tariff_plan_id,
                   dw_sub_subs_id,
                   subs_conn_rpt_dt,
                   subs_dconn_rpt_dt,
                   dw_prod_id,
                   dw_sub_prod_id,
                   dw_sf_position_id,
                   dw_physical_line_id,
                   dw_subs_rpt_stat_id,
                   dw_subs_stat_rsn_id
            FROM `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber`
            WHERE date(start_dttm) < x
        )s
        INNER JOIN
        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_basealldims_pstag` sbad
        ON
        s.dw_subs_id = sbad.dw_subs_id
        LEFT JOIN ( -- chnaged this join to left join , previously it was inner join
                    SELECT
                         dw_subs_rpt_stat_id,
                        subs_rpt_stat_cd,
                        start_dttm,
                        end_dttm
                    FROM
                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_reporting_status`
                    WHERE
                        (UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                        OR UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_inactive))
                        AND end_dttm IS NOT NULL
                )srs
        ON
        s.dw_subs_rpt_stat_id=srs.dw_subs_rpt_stat_id
        AND s.start_dttm BETWEEN srs.start_dttm AND srs.end_dttm
        INNER JOIN (
                    SELECT
                         dw_business_line_id,
                        business_line_lvl_2_cd,
                        start_dttm,
                        end_dttm,
                        business_line_lvl_1_cd,
                        business_line_lvl_3_cd
                    FROM
                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_business_line`
                    WHERE
                        (UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                        OR UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_mobile)
                        OR UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_rgu))
                        AND end_dttm IS NOT NULL
                ) bl
        ON
        s.dw_business_line_id= bl.dw_business_line_id
        AND s.start_dttm BETWEEN bl.start_dttm AND bl.end_dttm
        -- v9 changes New subs segment joins added-----
        LEFT JOIN
                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_segment_association` ssax

        ON
        s.dw_subs_id = ssax.dw_subs_id
        AND s.dw_sub_subs_id = ssax.dw_sub_subs_id
        AND s.start_dttm BETWEEN ssax.start_dttm AND ssax.end_dttm
        AND ssax.end_dttm is not null

        LEFT JOIN
                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_segment` ssegx

        ON
        ssax.dw_subs_seg_id=ssegx.dw_subs_seg_id
        AND ssax.start_dttm BETWEEN ssegx.start_dttm AND ssegx.end_dttm
        AND ssegx.end_dttm is not null;

        ---- v9 changes end

        --END: TEMP D_SUBSCRIBER TABLE CREATION--

        --START : Creating temp staging table f_subsbasesemd_history_from_src_stag with records for which history is fetched from History From Source Scenario.

        /* Target 1 is formed from selecting records which are not gross adds and their history is not available in target0.
        */
        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` PARTITION BY rpt_dt --###
         AS (
                        SELECT
                            *
                        FROM
                            `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_history_from_tgt_pstag`
                        WHERE
                            chk IS NULL
                    );

        --END: TEMP D_SUBSCRIBER TABLE CREATION--
        /*
        This set of joins is done to retrieve the last LoB.
        Tables Joined:
        1- Target table F Subscriber Base Semantic D : Base table used for subscribers that were not gross additions.
                                                    This is used as driver when bringing in historical data via any of the History-From- scenarios.
        2- D Subscriber : Filter on historical records and on business line id different from the current one.
                        Sort by start dttm desc and pick the last record (i.e. pick the latest LoB that is different from the current one).
        3- D Subscriber Reporting Status
        4- D Business Line : Filter on date and only take ids related to fixed and mobile connections

        */
        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_bl_pstag` PARTITION BY DATE(s1_end_dttm) --###
         AS (
                                SELECT
                                dw_subs_id,
                                s1_end_dttm,
                                IFNULL(s1_dw_business_line_id,
                                    to_hex(sha256("*NA"))) AS src_prev_bl
                                FROM (
                                SELECT
                                    *,
                                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY s1_start_dttm DESC, s1_dw_business_line_id) rn --row_number handling
                                FROM (
                                    SELECT
                                    target1.*,
                                    s1.start_dttm AS s1_start_dttm,
                                    s1.end_dttm AS s1_end_dttm,
                                    s1.dw_business_line_id AS s1_dw_business_line_id
                                    FROM
                                    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                    LEFT JOIN (
                                    SELECT
                                        start_dttm,
                                        dw_business_line_id,
                                        end_dttm,
                                        dw_subs_id
                                    FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                    WHERE
                                        UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                                        AND (UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                        OR UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_mobile))
                                        )s1
                                    ON
                                    target1.dw_subs_id=s1.dw_subs_id )
                                WHERE
                                    DATE(s1_start_dttm) < rpt_dt
                                    AND DATE(s1_end_dttm) < rpt_dt
                                    AND s1_dw_business_line_id NOT IN (to_hex(sha256("*NA")),
                                    dw_curr_business_line_id) )
                                WHERE
                                rn=1
                            );
        /*
        This set of joins is done to retrieve the last tariff.
        Tables Joined:
        1- Target table F Subscriber Base Semantic D : Base table used for subscribers that were not gross additions.
                                                    This is used as driver when bringing in historical data via any of the History-From- scenarios.
        2- D Subscriber :  Filter on historical records and on tariff id different from the current one.
                        Sort by start dttm desc and pick the last record (i.e. pick the latest tariff that is different from the current one).

        3- D Subscriber Reporting Status
        4- D Business Line :Filter on date and only take ids related to fixed and mobile connections

        */
        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_tp_pstag` PARTITION BY DATE(s2_end_dttm) --###
         AS (
                                SELECT
                                    dw_subs_id,
                                    s2_end_dttm,
                                    IFNULL(s2_dw_rpt_tariff_plan_id,
                                    to_hex(sha256("*NA"))) AS src_prev_tp
                                FROM (
                                    SELECT
                                    *,
                                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY s2_start_dttm DESC, s2_dw_rpt_tariff_plan_id) rn --row_number handling
                                    FROM (
                                    SELECT
                                        target1.*,
                                        s2.start_dttm AS s2_start_dttm,
                                        s2.end_dttm AS s2_end_dttm,
                                        s2.dw_rpt_tariff_plan_id AS s2_dw_rpt_tariff_plan_id
                                    FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                    LEFT JOIN (
                                        SELECT
                                        start_dttm,
                                        dw_rpt_tariff_plan_id,
                                        end_dttm,
                                        dw_subs_id
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                        WHERE
                                        UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)AND (UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                            OR UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_mobile)) )s2
                                    ON
                                        target1.dw_subs_id=s2.dw_subs_id )
                                    WHERE
                                    DATE(s2_start_dttm) < rpt_dt
                                    AND DATE(s2_end_dttm) < rpt_dt
                                    AND s2_dw_rpt_tariff_plan_id NOT IN (to_hex(sha256("*NA")),
                                        dw_curr_rpt_tariff_plan_id) )
                                WHERE
                                    rn=1
                            );


        /*
        This set of joins is done to retrieve the last segment.

        Tables Joined:
        1- Target table F Subscriber Base Semantic D : Base table used for subscribers that were not gross additions.
                                                    This is used as driver when bringing in historical data via any of the History-From- scenarios.
        2- D Subscriber Segment Association   : Filter on historical records. Filter on the subscriber segment used for group reporting.
                                            Filter on records that had a different (but still valid) segment Take the latest available record
        3- D Subscriber
        4- D Subscriber Reporting Status
        5- D Business Line
        6- D Subscriber Segment

        */
        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_seg_pstag` PARTITION BY DATE(ssa1_end_dttm) --###
         AS (
                                SELECT
                                dw_subs_id,
                                ssa1_end_dttm,
                                IFNULL(ssa1_dw_subs_seg_id,
                                    to_hex(sha256("*NA"))) AS src_prev_sseg
                                FROM (
                                SELECT
                                    *,
                                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY ssa1_start_dttm DESC,ssa1_dw_subs_seg_id) rn --row_number handling
                                FROM (
                                    SELECT
                                    target1.*,
                                    ssa1.start_dttm AS ssa1_start_dttm,
                                    ssa1.end_dttm AS ssa1_end_dttm,
                                    ssa1.dw_subs_seg_id AS ssa1_dw_subs_seg_id,
                                    ssa1.subs_seg_ss_type AS ssa1_subs_seg_ss_type,
                                    FROM
                                    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                    LEFT JOIN (
                                    SELECT
                                        ssa.dw_subs_id,
                                        ssa.start_dttm,
                                        ssa.dw_subs_seg_id,
                                        ssa.subs_seg_ss_type,
                                        ssa.end_dttm
                                    FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_segment_association` ssa
                                    INNER JOIN (
                                        SELECT
                                        start_dttm AS s3_start_dttm,
                                        end_dttm AS s3_end_dttm,
                                        dw_subs_id AS s3_dw_subs_id,
                                        dw_sub_subs_id AS s3_dw_sub_subs_id
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                        WHERE
                                        UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                                        AND (UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                            OR UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_mobile)) )s3
                                    ON
                                        ssa.dw_subs_id=s3_dw_subs_id
                                        AND ssa.dw_sub_subs_id = s3_dw_sub_subs_id
                                        AND ssa.start_dttm BETWEEN s3_start_dttm AND s3_end_dttm
                                    INNER JOIN
                                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_segment` sseg1
                                    ON
                                        ssa.dw_subs_seg_id=sseg1.dw_subs_seg_id
                                        AND ssa.start_dttm BETWEEN sseg1.start_dttm AND sseg1.end_dttm
                                        and ssa.end_dttm is not null)ssa1
                                    ON
                                    target1.dw_subs_id=ssa1.dw_subs_id )
                                WHERE
                                    DATE(ssa1_start_dttm) < rpt_dt
                                    and DATE(ssa1_end_dttm) < rpt_dt
                                    AND UPPER(ssa1_subs_seg_ss_type) IN UNNEST(v_subs_seg_for_reporting)
                                    AND ssa1_dw_subs_seg_id NOT IN ( to_hex(sha256("*NA")),
                                    dw_curr_subs_seg_id) )
                                WHERE
                                rn=1
                            );

        /*
        This set of joins is done to retrieve the acquisition detail.

        Tables Joined:
        1- Target table F Subscriber Base Semantic D : Base table used for subscribers that were not gross additions.
                                                    This is used as driver when bringing in historical data via any of the History-From- scenarios.
        2- D Subscriber :
        3- D Subscriber Reporting Status
        4- D Business Line
        5- D Sales Force Position
        6- D Port Request Detail
        7- D Port Request
        8- D Port Request Type
        9- D Port Request Status
        10- D Port Request Status Reason

        */
        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_aq_pstag` PARTITION BY s4_subs_conn_rpt_dt --###
         AS (
                                    SELECT
                                    dw_subs_id,
                                    s4_dw_sf_position_id,
                                    s4_dw_prod_id,
                                    s4_dw_sub_prod_id,
                                    sfp2_dw_sf_type_id,
                                    s4_subs_conn_rpt_dt,
                                    prt1_port_req_type_cd,
                                    prs1_port_req_stat_lvl_1_cd,
                                    prsr1_port_req_stat_rsn_lvl_1_cd,
									pr1_port_req_completed_dttm, --NPP-19714
                                    pr1_dw_donor_operator_id,
                                    s4_dw_business_line_id, -- v9 changes
                                    s4_dw_rpt_tariff_plan_id -- v9 changes
                                    FROM (
                                    SELECT
                                        *,
                                        ROW_NUMBER() OVER (PARTITION BY dw_subs_id,prt1_port_req_type_cd ORDER BY s4_start_dttm DESC,prt1_port_req_type_cd, pr1_dw_port_req_stat_id) rn --row_number handling --NPP-19714
                                    FROM (
                                        SELECT
                                        target1.*,
                                        s4_start_dttm,
                                        s4_dw_sf_position_id,
                                        s4_dw_subs_id,
                                        s4_dw_prod_id,
                                        s4_dw_sub_prod_id,
                                        sfp2_dw_sf_type_id,
                                        s4_subs_conn_rpt_dt,
                                        prt1_port_req_type_cd,
                                        prs1_port_req_stat_lvl_1_cd,
                                        prsr1_port_req_stat_rsn_lvl_1_cd,
                                        pr1_dw_port_req_stat_id, --###
                                        pr1_dw_donor_operator_id,
										pr1_port_req_completed_dttm, --NPP-19714
                                        s4_dw_business_line_id, -- v9 changes
                                        s4_dw_rpt_tariff_plan_id -- v9 changes
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                        LEFT JOIN (
                                        SELECT
                                            s.subs_conn_rpt_dt AS s4_subs_conn_rpt_dt,
                                            s.start_dttm AS s4_start_dttm,
                                            s.end_dttm AS s4_end_dttm,
                                            s.dw_subs_id AS s4_dw_subs_id,
                                            s.dw_prod_id AS s4_dw_prod_id,
                                            s.dw_sub_prod_id AS s4_dw_sub_prod_id,
                                            s.dw_sf_position_id AS s4_dw_sf_position_id,
                                            sfp2.dw_sf_type_id AS sfp2_dw_sf_type_id,
                                            prt1_port_req_type_cd,
                                            prs1_port_req_stat_lvl_1_cd,
											pr1_port_req_completed_dttm, --NPP-19714
                                            prsr1_port_req_stat_rsn_lvl_1_cd,
                                            pr1_dw_port_req_stat_id, --###
                                            pr1_dw_donor_operator_id,
                                            s.dw_business_line_id AS s4_dw_business_line_id, -- v9 changes
                                            s.dw_rpt_tariff_plan_id AS s4_dw_rpt_tariff_plan_id -- v9 changes

                                        FROM (
                                            SELECT
                                            subs_conn_rpt_dt,
                                            start_dttm,
                                            end_dttm,
                                            dw_sub_subs_id,
                                            dw_subs_id,
                                            dw_prod_id,
                                            dw_sub_prod_id,
                                            dw_sf_position_id,
                                            dw_business_line_id, -- v9 changes
                                            dw_rpt_tariff_plan_id -- v9 changes
                                            FROM
                                            `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                            WHERE
                                            UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                                            AND (UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                                OR UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_mobile))) s
                                        LEFT JOIN -- workaround as left join due to sales force potential CR, originally it is inner join
                                            `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_sales_force_position` sfp2
                                        ON
                                            s.dw_sf_position_id=sfp2.dw_sf_position_id
                                            AND s.start_dttm BETWEEN sfp2.start_dttm AND sfp2.end_dttm
                                            AND sfp2.end_dttm is not null
                                        LEFT JOIN (
                                            SELECT
                                            prd.dw_subs_id AS prd1_dw_subs_id,
                                            prd.dw_sub_subs_id AS prd1_dw_sub_subs_id,
                                            prd.start_dttm AS prd1_start_dttm,
                                            prd.end_dttm AS prd1_end_dttm,
                                            prt1_port_req_type_cd,
                                            prs1_port_req_stat_lvl_1_cd,
                                            prsr1_port_req_stat_rsn_lvl_1_cd,
                                            pr1_dw_port_req_stat_id, --###
                                            pr1_dw_donor_operator_id,
                                            prt1_end_dttm,
                                            prt1_start_dttm,
                                            pr1_start_dttm,
                                            pr1_end_dttm,
											pr1_port_req_completed_dttm, --NPP-19714
                                            prs1_end_dttm,
                                            prs1_start_dttm,
                                            prsr1_end_dttm,
                                            prsr1_start_dttm
                                            FROM
                                            `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request_detail` prd
                                            INNER JOIN (
                                            SELECT
                                                pr.dw_port_req_id,
                                                pr.dw_port_req_stat_id AS pr1_dw_port_req_stat_id, --###
                                                pr.start_dttm AS pr1_start_dttm,
                                                pr.end_dttm AS pr1_end_dttm,
												pr.port_req_completed_dttm AS pr1_port_req_completed_dttm,--NPP-19714
                                                prt1.port_req_type_cd AS prt1_port_req_type_cd,
                                                prs1.port_req_stat_lvl_1_cd AS prs1_port_req_stat_lvl_1_cd,
                                                prsr1.port_req_stat_rsn_lvl_1_cd AS prsr1_port_req_stat_rsn_lvl_1_cd,
                                                pr.dw_donor_operator_id AS pr1_dw_donor_operator_id,
                                                prt1.start_dttm AS prt1_start_dttm,
                                                prt1.end_dttm AS prt1_end_dttm,
                                                prs1.start_dttm AS prs1_start_dttm,
                                                prs1.end_dttm AS prs1_end_dttm,
                                                prsr1.start_dttm AS prsr1_start_dttm,
                                                prsr1.end_dttm AS prsr1_end_dttm
                                            FROM
                                                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request` pr
                                            INNER JOIN
                                                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request_type` prt1
                                            ON
                                                pr.dw_port_req_type_id=prt1.dw_port_req_type_id
                                            INNER JOIN
                                                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request_status` prs1
                                            ON
                                                prs1.dw_port_req_stat_id = pr.dw_port_req_stat_id
                                            INNER JOIN
                                                `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_port_request_status_reason` prsr1
                                            ON
                                                prsr1.dw_port_req_stat_rsn_id= pr.dw_port_req_stat_rsn_id
                                            where pr.end_dttm is not null
                                            and prt1.end_dttm is not null
                                            and prs1.end_dttm is not null
                                            and prsr1.end_dttm is not null
                                            )pr1
                                            ON
                                            prd.dw_port_req_id=pr1.dw_port_req_id
                                            and prd.end_dttm is not null
                                            )prd1
                                        ON
                                            s.dw_subs_id = prd1_dw_subs_id
                                            AND s.dw_sub_subs_id = prd1_dw_sub_subs_id
										-->> NPP-26549
                                        	AND s.start_dttm BETWEEN prd1_start_dttm AND prd1_end_dttm
                                            AND s.start_dttm BETWEEN prt1_start_dttm AND prt1_end_dttm
                                            AND s.start_dttm BETWEEN pr1_start_dttm AND pr1_end_dttm
                                            AND s.start_dttm BETWEEN prs1_start_dttm AND prs1_end_dttm
                                            AND s.start_dttm BETWEEN prsr1_start_dttm AND prsr1_end_dttm
										/*WHERE
                                            s.start_dttm BETWEEN prd1_start_dttm AND prd1_end_dttm
                                            AND s.start_dttm BETWEEN prt1_start_dttm AND prt1_end_dttm
                                            AND s.start_dttm BETWEEN pr1_start_dttm AND pr1_end_dttm
                                            AND s.start_dttm BETWEEN prs1_start_dttm AND prs1_end_dttm
                                            AND s.start_dttm BETWEEN prsr1_start_dttm AND prsr1_end_dttm */
									    --<< NPP-26549
                                            )s4
                                        ON
                                        target1.dw_subs_id=s4_dw_subs_id )
                                    WHERE
                                        DATE(s4_start_dttm) = s4_subs_conn_rpt_dt )
                                    WHERE
                                    rn=1
                            );
        /*
        This set of joins is done to retrieve the last retention detail..

        Tables Joined:
        1- Target table F Subscriber Base Semantic D : Base table used for subscribers that were not gross additions.
                                                    This is used as driver when bringing in historical data via any of the History-From- scenarios.
        2- D Customer Agreement
        3- D Customer Agreement Classification
        4- D Customer Agreement Type
        5- D Subscriber
        6- D Subscriber Reporting Status
        7- D Business Line
        */
        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_ret_pstag`
         AS (
                                SELECT
                                    dw_subs_id,
                                    CASE
                                        WHEN retention_ind IS NULL
                                            AND agreement_count > 0
                                           -- AND subs_agmt_last_term_dt > max_cust_agmt_term_rpt_dt
                                        THEN 1
                                        ELSE 0
                                    END AS retention_ind
                                FROM (
                                    SELECT
                                    target1.*,
                                    max_cust_agmt_term_rpt_dt
                                    FROM
                                    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                    LEFT JOIN (
                                    SELECT
                                        MAX(cagr.cust_agmt_term_rpt_dt) OVER(PARTITION BY cagr.dw_subs_id) AS max_cust_agmt_term_rpt_dt,
                                        cagr.dw_subs_id,
                                        cagr.start_dttm
                                    FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_agreement` cagr
                                    INNER JOIN
                                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_agreement_classification` cagrc1
                                    ON
                                        cagr.dw_cust_agmt_class_id= cagrc1.dw_cust_agmt_class_id
                                        AND cagr.start_dttm BETWEEN cagrc1.start_dttm
                                        AND cagrc1.end_dttm
                                        AND UPPER(cagrc1.cust_agmt_class_cd) IN UNNEST(v_agmt_class_retention)
                                    INNER JOIN
                                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_customer_agreement_type` cagrt0
                                    ON
                                        cagr.dw_cust_agmt_type_id= cagrt0.dw_cust_agmt_type_id
                                        AND cagr.start_dttm BETWEEN cagrt0.start_dttm
                                        AND cagrt0.end_dttm
                                        AND UPPER(cagrt0.cust_agmt_type_lvl_2_cd) IN UNNEST(v_agmt_type_with_commitment)
                                    INNER JOIN (
                                        SELECT
                                        start_dttm,
                                        dw_business_line_id,
                                        end_dttm,
                                        dw_subs_id,
                                        dw_sub_subs_id
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                        WHERE
                                        UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                                        AND (UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                            OR UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_mobile)) )s5
                                    ON
                                        s5.dw_subs_id = cagr.dw_subs_id
                                        AND s5.dw_subs_id = cagr.dw_sub_subs_id
                                        AND cagr.start_dttm BETWEEN s5.start_dttm
                                        AND s5.end_dttm
                                    where cagrt0.end_dttm is not NULL
                                    and cagrc1.end_dttm is not NULL
                                    and cagr.end_dttm is not null)cagr0
                                    ON
                                    target1.dw_subs_id=cagr0.dw_subs_id
                                    WHERE
                                    DATE(cagr0.start_dttm) < target1.rpt_dt
                                    )
                            );

        /*
        The below set of joins is done to retrieve the last Fixed Line Type from EIM Source.

        Tables Joined:
        1- Target table F Subscriber Base Semantic D : Base table used for subscribers that were not gross additions.
                                                    This is used as driver when bringing in historical data via any of the History-From- scenarios.
        2- D Subscriber : Filter on historical records and on Fixed Line Type id different from the current one.
                        Sort by start dttm desc and pick the last record (i.e. pick the latest Fixed Line Type that is different from the current one).
        3- D Physical Line  : Filter on historical records. Filter on records that had a different (but still valid) Fixed Line Type. Take the latest available record
        4- D Subscriber Reporting Status
        5- D Business Line
        */
        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_flt_pstag` PARTITION BY DATE(s7_end_dttm) --###
         AS (
                                SELECT
                                dw_subs_id,
                                s7_end_dttm,
                                pl2_end_dttm,
                                IFNULL(pl2_dw_fixed_line_type_id,
                                    to_hex(sha256("*NA"))) AS src_prev_flt
                                FROM (
                                SELECT
                                    *,
                                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY pl2_start_dttm DESC,pl2_dw_fixed_line_type_id) rn --row_number handling
                                FROM (
                                    SELECT
                                    target1.*,
                                    s7_start_dttm,
                                    s7_end_dttm,
                                    s7_dw_subs_id,
                                    pl2_dw_fixed_line_type_id,
                                    pl2_start_dttm,
                                    pl2_end_dttm
                                    FROM
                                    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                    LEFT JOIN (
                                    SELECT
                                        s7.dw_subs_id AS s7_dw_subs_id,
                                        s7.start_dttm AS s7_start_dttm,
                                        s7.end_dttm AS s7_end_dttm,
                                        pl2.start_dttm AS pl2_start_dttm,
                                        pl2.dw_fixed_line_type_id AS pl2_dw_fixed_line_type_id,
                                        pl2.end_dttm AS pl2_end_dttm
                                    FROM (
                                        SELECT
                                        start_dttm,
                                        dw_business_line_id,
                                        end_dttm,
                                        dw_subs_id,
                                        dw_physical_line_id
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                        WHERE
                                        UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                                        AND UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_fixed) )s7
                                    INNER JOIN (
                                        SELECT
                                        dw_physical_line_id,
                                        start_dttm,
                                        dw_fixed_line_type_id,
                                        end_dttm,
                                        row_number() over(partition by dw_physical_line_id order by start_dttm desc, end_dttm desc) pl_rn -- WORKAROUND TO HANDLE DUPLICATE RECORDS IN D_PHYSICAL LINE
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_physical_line`
                                        where DATE(start_dttm) < x
                                            AND DATE(end_dttm) < x) pl2
                                    ON
                                        s7.dw_physical_line_id= pl2.dw_physical_line_id
                                        and pl_rn = 1)
                                    ON
                                    target1.dw_subs_id = s7_dw_subs_id)
                                WHERE
                                    DATE(s7_start_dttm) < rpt_dt
                                    AND pl2_dw_fixed_line_type_id NOT IN (to_hex(sha256("*NA")),
                                    dw_curr_fixed_line_type_id) )
                                WHERE
                                rn=1
                            );

    ---- v9 changes begin
    CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_acq_seg_pstag`
     AS (
                                SELECT
                                    dw_subs_id,
                                    IFNULL(s6_dw_subs_seg_id,to_hex(sha256("*NA"))) AS src_prev_acq_seg,
                                FROM (
                                    SELECT
                                    *,
                                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY s6_start_dttm DESC,s6_dw_subs_seg_id) rn --row_number handling
                                    FROM (
                                    SELECT
                                        target1.*,
                                        s6.start_dttm AS s6_start_dttm,
                                        s6.dw_subs_seg_id AS s6_dw_subs_seg_id,
                                    FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                    LEFT JOIN (
                                        SELECT
                                        start_dttm,
                                        dw_subs_seg_id,
                                        dw_subs_id
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                        WHERE
                                        date(start_dttm) = subs_conn_rpt_dt
                                        AND UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                                        AND (UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                        OR UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_mobile))
                                        AND UPPER(subs_seg_ss_type) IN UNNEST(v_subs_seg_for_reporting)
                                        )s6
                                    ON
                                        target1.dw_subs_id=s6.dw_subs_id
                                    )
                                )
                                WHERE
                                    rn=1
                            );
        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dvc_pstag` PARTITION BY DATE(s7_end_dttm) --###
       AS (
                            SELECT
                            dw_subs_id,
                            s7_end_dttm,
                            da1_end_dttm,
                            IFNULL(da1_dw_dvc_id, to_hex(sha256("*NA"))) AS src_prev_dvc_id
                            FROM (
                                    SELECT
                                    *,
                                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY da1_start_dttm DESC,da1_dw_dvc_id) rn --row_number handling
                                    FROM (
                                            SELECT
                                                target1.*,
                                                s7_start_dttm,
                                                s7_end_dttm,
                                                s7_dw_subs_id,
                                                da1_dw_dvc_id,
                                                da1_start_dttm,
                                                da1_end_dttm
                                            FROM
                                                `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                            LEFT JOIN (
                                                SELECT
                                                s7.dw_subs_id AS s7_dw_subs_id,
                                                s7.start_dttm AS s7_start_dttm,
                                                s7.end_dttm AS s7_end_dttm,
                                                da1.start_dttm AS da1_start_dttm,
                                                da1.dw_dvc_id AS da1_dw_dvc_id,
                                                da1.end_dttm AS da1_end_dttm
                                                FROM (
                                                        SELECT
                                                            start_dttm,
                                                            end_dttm,
                                                            dw_subs_id,
                                                            dw_sub_subs_id
                                                        FROM
                                                            `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                                        WHERE
                                                            UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
                                                            AND (UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_fixed)
                                                            OR UPPER(bl_business_line_lvl_1_cd) IN UNNEST(v_bl_mobile))
                                                    )s7
                                                LEFT JOIN (
                                                                SELECT
                                                                    * EXCEPT (rnk)
                                                                FROM (
                                                                    SELECT
                                                                    DISTINCT dw_subs_id,
                                                                    dw_sub_subs_id,
                                                                    dw_dvc_id,
                                                                    start_dttm,
                                                                    end_dttm,
                                                                    ROW_NUMBER() OVER(PARTITION BY dw_subs_id, dw_sub_subs_id ORDER BY is_dvc_with_main_sim DESC, start_dttm DESC, end_dttm DESC) rnk -- added dw_sub_subs_id in partition by clause
                                                                    FROM
                                                                    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_device_association`
                                                                    WHERE
                                                                    DATE(start_dttm) < x
                                                                    AND DATE(end_dttm) < x )
                                                                WHERE
                                                                    rnk =1
                                                        )da1
                                                ON
                                                s7.dw_subs_id = da1.dw_subs_id
                                                AND s7.dw_sub_subs_id = da1.dw_sub_subs_id
                                            )
                                            ON
                                                target1.dw_subs_id = s7_dw_subs_id
                                        )
                                            WHERE
                                            DATE(da1_start_dttm) < rpt_dt
                                            AND da1_dw_dvc_id NOT IN (to_hex(sha256("*NA")),dw_curr_primary_dvc_id)
                                )
                            WHERE rn=1
                        );

            CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dconn_pstag`
         AS (
                                SELECT
                                    dw_subs_id,
                                    IFNULL(s9_dw_rpt_tariff_plan_id,to_hex(sha256("*NA"))) AS src_prev_dconn_tp,
                                    IFNULL(s9_dw_business_line_id,to_hex(sha256("*NA"))) AS src_prev_dconn_bl,
                                    IFNULL(s9_dw_subs_stat_rsn_id,to_hex(sha256("*NA"))) AS src_prev_dconn_stat_rsn,

                                FROM (
                                    SELECT
                                    *,
                                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY s9_start_dttm DESC) rn
                                    FROM (
                                    SELECT
                                        target1.*,
                                        s9.start_dttm AS s9_start_dttm,
                                        s9.end_dttm AS s9_end_dttm,
                                        s9.subs_dconn_rpt_dt AS s9_subs_dconn_rpt_dt,
                                        s9.dw_rpt_tariff_plan_id AS  s9_dw_rpt_tariff_plan_id,
                                        s9.dw_business_line_id  AS   s9_dw_business_line_id,
                                        s9.dw_subs_stat_rsn_id  AS   s9_dw_subs_stat_rsn_id
                                    FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                    LEFT JOIN (
                                        SELECT
                                        start_dttm,
                                        dw_rpt_tariff_plan_id,
                                        dw_business_line_id,
                                        dw_subs_stat_rsn_id,
                                        subs_dconn_rpt_dt,
                                        end_dttm,
                                        dw_subs_id
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                        WHERE
                                        date(start_dttm) = subs_dconn_rpt_dt )s9
                                    ON
                                        target1.dw_subs_id=s9.dw_subs_id
                                    )
                                )
                                WHERE
                                    rn=1
                            );

        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dconn_seg_pstag`
         AS (
                                SELECT
                                    dw_subs_id,
                                    IFNULL(s10_dw_subs_seg_id,to_hex(sha256("*NA"))) AS src_prev_dconn_seg,
                                FROM (
                                    SELECT
                                    *,
                                    ROW_NUMBER() OVER (PARTITION BY dw_subs_id ORDER BY s10_start_dttm DESC) rn
                                    FROM (
                                    SELECT
                                        target1.*,
                                        s10.start_dttm AS s10_start_dttm,
                                        s10.dw_subs_seg_id AS s10_dw_subs_seg_id,
                                    FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
                                    LEFT JOIN (
                                        SELECT
                                        start_dttm,
                                        dw_subs_seg_id,
                                        dw_subs_id,
                                        subs_dconn_rpt_dt
                                        FROM
                                        `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dsubs_stag`
                                        WHERE
                                        date(start_dttm) = subs_dconn_rpt_dt
                                        AND UPPER(subs_seg_ss_type) IN UNNEST(v_subs_seg_for_reporting)
                                        )s10
                                    ON
                                        target1.dw_subs_id=s10.dw_subs_id
                                    )
                                )
                                WHERE
                                    rn=1
                            );

        ----- v9 changes end

        --START : Creating temp staging table f_subsbasesemd_history_from_src_stag with records for which history is fetched from History From Source Scenario.

        CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_history_from_src_pstag` PARTITION BY rpt_dt AS
        SELECT
            DISTINCT *,
            CASE
            WHEN retention_ind=1 THEN t1_dw_last_retention_sf_position_id
            ELSE to_hex(sha256("*NA"))
            END AS dw_last_retention_sf_position_id,
            CASE
            WHEN retention_ind=1 THEN t1_dw_last_retention_sf_type_id
            ELSE to_hex(sha256("*NA"))
            END AS dw_last_retention_sf_type_id,
            CASE
            WHEN retention_ind=1 THEN rpt_dt
            ELSE CAST(NULL AS date)
            END AS subs_last_retention_dt,
            CASE
            WHEN DATE(last_business_line_change_dt)= rpt_dt THEN 1
            ELSE 0
            END AS business_line_change_ind,
            CASE
            WHEN DATE(last_rpt_tariff_plan_change_dt) = rpt_dt THEN 1
            ELSE 0
            END AS tariff_plan_change_ind,
            CASE
            WHEN DATE(last_subs_seg_change_dt)= rpt_dt THEN 1
            ELSE 0
            END AS subs_seg_change_ind,
            --v9 changes BEGIN
            CASE
            WHEN DATE(last_primary_dvc_change_dt)= rpt_dt THEN 1
            ELSE 0
            END AS primary_dvc_change_ind
            --v9 changes end

        FROM (
            SELECT
            target1.business_line_lvl_2_cd,
            target1.subs_seg_lvl_1_cd,
            target1.dconn_ind,
            target1.dw_curr_business_line_id,
            target1.dw_curr_rpt_tariff_plan_id,
            target1.dw_curr_subs_seg_id,
            target1.dw_cust_acct_geo_id,
            target1.dw_cust_acct_id,
            target1.dw_cust_geo_id,
            target1.dw_cust_id,
            target1.dw_cust_sector_id,
            target1.dw_cust_seg_id,
            target1.dw_cust_type_id,
            target1.dw_physical_line_id,
            target1.dw_port_out_operator_id,
            target1.dw_port_req_stat_id,
            target1.dw_port_req_stat_rsn_id,
            target1.dw_curr_primary_dvc_cat_id,
            target1.dw_curr_primary_dvc_id,
            target1.dw_subs_barring_stat_id,
            target1.dw_subs_geo_id,
            target1.dw_subs_id,
            target1.dw_subs_rpt_stat_id,
            target1.dw_subs_stat_id,
            target1.dw_subs_stat_rsn_id,
            target1.dw_uniq_current_business_line_id,
            target1.dw_uniq_current_rpt_tariff_plan_id,
            target1.dw_uniq_current_subs_seg_id,
            target1.dw_uniq_cust_acct_geo_id,
            target1.dw_uniq_cust_acct_id,
            target1.dw_uniq_cust_geo_id,
            target1.dw_uniq_cust_id,
            target1.dw_uniq_cust_sector_id,
            target1.dw_uniq_cust_seg_id,
            target1.dw_uniq_cust_type_id,
            target1.dw_uniq_port_out_operator_id,
            target1.dw_uniq_port_req_stat_id,
            target1.dw_uniq_port_req_stat_rsn_id,
            target1.dw_uniq_curr_primary_dvc_cat_id,
            target1.dw_uniq_curr_primary_dvc_id,
            target1.dw_uniq_subs_barring_stat_id,
            target1.dw_uniq_subs_geo_id,
            target1.dw_uniq_subs_id,
            target1.dw_uniq_subs_rpt_stat_id,
            target1.dw_uniq_subs_stat_id,
            target1.dw_uniq_subs_stat_rsn_id,
            target1.gross_add_ind,
            target1.is_dummy_cust,
            target1.is_in_active_base,
            target1.is_in_closing_base,
            target1.is_in_contract_commitment,
            target1.is_port_out,
            target1.last_chargeable_event_dt,
            target1.migr_of_addr_ind,
            target1.migr_of_tech_ind,
            target1.reconnection_ind,
            target1.rpt_dt,
            target1.subs_barring_dt,
            target1.subs_conn_num,
            target1.subs_conn_rpt_dt,
            target1.subs_dconn_rpt_dt,
            target1.subs_preactivation_rpt_dt,
            target1.subs_reconnection_rpt_dt,
            target1.subs_rgstn_dt,
            target1.subs_agmt_last_term_dt,
            target1.with_commitment,
            target1.dw_last_retention_sf_position_id AS t1_dw_last_retention_sf_position_id,
            target1.dw_last_retention_sf_type_id AS t1_dw_last_retention_sf_type_id,
            h_bl.src_prev_bl AS dw_prev_business_line_id,
            CASE
                WHEN h_bl.src_prev_bl = to_hex(sha256("*NA")) THEN CAST(NULL AS DATE)
                ELSE DATE(TIMESTAMP_ADD(h_bl.s1_end_dttm, INTERVAL 1 SECOND))
            END AS last_business_line_change_dt,
            h_tp.src_prev_tp AS dw_prev_rpt_tariff_plan_id,
            CASE
                WHEN h_tp.src_prev_tp = to_hex(sha256("*NA")) THEN CAST(NULL AS DATE)
                ELSE DATE(TIMESTAMP_ADD(h_tp.s2_end_dttm, INTERVAL 1 SECOND))
            END AS last_rpt_tariff_plan_change_dt,
            h_sg.src_prev_sseg AS dw_prev_subs_seg_id,
            CASE
                WHEN h_sg.src_prev_sseg = to_hex(sha256("*NA")) THEN CAST(NULL AS DATE)
                ELSE DATE(TIMESTAMP_ADD(h_sg.ssa1_end_dttm, INTERVAL 1 SECOND))
            END AS last_subs_seg_change_dt,
            h_aq.s4_dw_sf_position_id AS dw_acq_sf_position_id,
            h_aq.sfp2_dw_sf_type_id AS dw_acq_sf_type_id,
            h_aq.s4_dw_prod_id AS dw_first_acquired_prod_id,
            h_aq.s4_dw_sub_prod_id AS dw_sub_first_acquired_prod_id,
            CASE
                WHEN UPPER(h_aq.prt1_port_req_type_cd) IN UNNEST(v_port_req_type_in)
                AND UPPER(h_aq.prs1_port_req_stat_lvl_1_cd )IN UNNEST(v_port_req_stat_success)
                OR UPPER(h_aq.prsr1_port_req_stat_rsn_lvl_1_cd) IN UNNEST(v_port_req_stat_rsn_success) THEN 1
                ELSE 0
            END AS is_port_in,
			--NPP-19714<
			CASE
                WHEN UPPER(h_aq.prt1_port_req_type_cd) IN UNNEST(v_port_req_type_in)
					AND UPPER(h_aq.prs1_port_req_stat_lvl_1_cd )IN UNNEST(v_port_req_stat_success)
					OR UPPER(h_aq.prsr1_port_req_stat_rsn_lvl_1_cd) IN UNNEST(v_port_req_stat_rsn_success)
				THEN DATE(h_aq.pr1_port_req_completed_dttm) ELSE CAST(NULL AS DATE)
			END AS port_in_completion_dt,
			CASE
                WHEN UPPER(h_aq.prt1_port_req_type_cd) IN UNNEST(v_port_req_type_out)
					AND UPPER(h_aq.prs1_port_req_stat_lvl_1_cd )IN UNNEST(v_port_req_stat_success)
					OR UPPER(h_aq.prsr1_port_req_stat_rsn_lvl_1_cd) IN UNNEST(v_port_req_stat_rsn_success)
				THEN DATE(h_aq.pr1_port_req_completed_dttm) ELSE CAST(NULL AS DATE)
			END AS port_out_completion_dt,
			-->NPP-19714
            CASE
                WHEN UPPER(h_aq.prt1_port_req_type_cd) IN UNNEST(v_port_req_type_in)
                AND UPPER(h_aq.prs1_port_req_stat_lvl_1_cd) IN UNNEST(v_port_req_stat_success)
                OR UPPER(h_aq.prsr1_port_req_stat_rsn_lvl_1_cd) IN UNNEST(v_port_req_stat_rsn_success) THEN pr1_dw_donor_operator_id
                ELSE to_hex(sha256("*NA"))
            END AS dw_port_in_operator_id,
            h_ret.retention_ind,
            dw_uniq_physical_line_id,
            dw_curr_fixed_line_type_id,
            dw_uniq_curr_fixed_line_type_id,
            src_prev_flt AS dw_prev_fixed_line_type_id,
            inactive_to_active_change_dt,
            active_to_inactive_change_dt,
            CASE
                WHEN h_flt.src_prev_flt = to_hex(sha256("*NA")) THEN CAST(NULL AS DATE)
            ELSE
            DATE(TIMESTAMP_ADD(h_flt.pl2_end_dttm, INTERVAL 1 SECOND))
            END
            AS last_fixed_line_type_change_dt,
            -- v9 changes begin
            h_dvc.src_prev_dvc_id as dw_prev_primary_dvc_id,
            h_aq.s4_dw_business_line_id as dw_acq_business_line_id,
            h_acq_seg.src_prev_acq_seg as dw_acq_subs_seg_id,
            h_aq.s4_dw_rpt_tariff_plan_id as dw_acq_rpt_tariff_plan_id,
            --v10 changes begin--
            CASE
                WHEN target1.dw_dconn_business_line_id = to_hex(sha256("*NA"))
                THEN h_dconn.src_prev_dconn_bl
                ELSE target1.dw_dconn_business_line_id
            END AS dw_dconn_business_line_id,
            CASE
                WHEN target1.dw_dconn_rpt_tariff_plan_id = to_hex(sha256("*NA"))
                THEN h_dconn.src_prev_dconn_tp
                ELSE target1.dw_dconn_rpt_tariff_plan_id
            END AS dw_dconn_rpt_tariff_plan_id,
            CASE
                WHEN target1.dw_dconn_stat_rsn_id = to_hex(sha256("*NA"))
                THEN h_dconn.src_prev_dconn_stat_rsn
                ELSE target1.dw_dconn_stat_rsn_id
            END AS dw_dconn_stat_rsn_id,
            CASE
                WHEN target1.dw_dconn_subs_seg_id = to_hex(sha256("*NA"))
                THEN h_dconn_seg.src_prev_dconn_seg
                ELSE target1.dw_dconn_subs_seg_id
            END AS dw_dconn_subs_seg_id,
            /*h_dconn.src_prev_dconn_bl as dw_dconn_business_line_id,
            h_dconn.src_prev_dconn_tp as dw_dconn_rpt_tariff_plan_id,
            h_dconn.src_prev_dconn_stat_rsn as dw_dconn_stat_rsn_id,
            h_dconn_seg.src_prev_dconn_seg as dw_dconn_subs_seg_id,*/
            --v10 changes end--
            CASE
                WHEN h_dvc.src_prev_dvc_id = to_hex(sha256("*NA")) THEN CAST(NULL AS DATE)
            ELSE
            DATE(TIMESTAMP_ADD(h_dvc.da1_end_dttm, INTERVAL 1 SECOND))
            END
            AS last_primary_dvc_change_dt,

            -- v9 changes end
			ss_cd --v12 changes
            FROM
            `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_target1_pstag` target1
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_bl_pstag` h_bl
            ON
            target1.dw_subs_id= h_bl.dw_subs_id
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_tp_pstag` h_tp
            ON
            target1.dw_subs_id = h_tp.dw_subs_id
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_seg_pstag` h_sg
            ON
            target1.dw_subs_id = h_sg.dw_subs_id
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_aq_pstag` h_aq
            ON
            target1.dw_subs_id = h_aq.dw_subs_id
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_ret_pstag` h_ret
            ON
            target1.dw_subs_id = h_ret.dw_subs_id
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_flt_pstag` h_flt
            ON
            target1.dw_subs_id = h_flt.dw_subs_id
            --v9 changes begin
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dvc_pstag` h_dvc
            ON
            target1.dw_subs_id = h_dvc.dw_subs_id
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_acq_seg_pstag` h_acq_seg
            ON
            target1.dw_subs_id = h_acq_seg.dw_subs_id
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dconn_pstag` h_dconn
            ON
            target1.dw_subs_id = h_dconn.dw_subs_id
            LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_histfrmsrc_dconn_seg_pstag` h_dconn_seg
            ON
            target1.dw_subs_id = h_dconn_seg.dw_subs_id
            -- v9 changes end
            )
             ;
    END IF ;
/*IF ELSE BLOCKS ENDS: HISTORY FROM SOURCE SCENARIO */


--END : Creating temp staging table f_subsbasesemd_history_from_src_stag with records for which history is fetched from History From Source Scenario.

 ----------------RECORDS WITH  GROSS ADDS NOT EQUAL TO 1------------
 ----------------------HISTORY FROMM SOURCE-------------------------
 -----************************ENDD**************************--------


 ------------------- BEGIN : COMBINE GROSS_ADD, HIST FROM TARGET,HISTOR FROM SOURCE------------------------
 /*In this step, the output of all 3 scenarios (i.e records where gross_add=1, gross_add <> 1 & history is present in target table,
 gross_add <>1 and history needs to be taken from EIM source tables) is combined in one staging table f_subsbasesemd_combined_stag */

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_combined_pstag` PARTITION BY rpt_dt AS
SELECT
  business_line_change_ind,
  business_line_lvl_2_cd,
  dconn_ind,
  subs_seg_lvl_1_cd,
  dw_acq_sf_position_id,
  dw_acq_sf_type_id,
  dw_curr_business_line_id,
  dw_curr_rpt_tariff_plan_id,
  dw_curr_subs_seg_id,
  dw_cust_acct_geo_id,
  dw_cust_acct_id,
  dw_cust_geo_id,
  dw_cust_id,
  dw_cust_sector_id,
  dw_cust_seg_id,
  dw_cust_type_id,
  dw_first_acquired_prod_id,
  dw_last_retention_sf_position_id,
  dw_last_retention_sf_type_id,
  dw_physical_line_id,
  dw_port_in_operator_id,
  dw_port_out_operator_id,
  dw_port_req_stat_id,
  dw_port_req_stat_rsn_id,
  dw_prev_business_line_id,
  dw_prev_rpt_tariff_plan_id,
  dw_prev_subs_seg_id,
  dw_curr_primary_dvc_cat_id,
  dw_curr_primary_dvc_id,
  dw_sub_first_acquired_prod_id,
  dw_subs_barring_stat_id,
  dw_subs_geo_id,
  dw_subs_id,
  dw_subs_rpt_stat_id,
  dw_subs_stat_id,
  dw_subs_stat_rsn_id,
  dw_uniq_current_business_line_id,
  dw_uniq_current_rpt_tariff_plan_id,
  dw_uniq_current_subs_seg_id,
  dw_uniq_cust_acct_geo_id,
  dw_uniq_cust_acct_id,
  dw_uniq_cust_geo_id,
  dw_uniq_cust_id,
  dw_uniq_cust_sector_id,
  dw_uniq_cust_seg_id,
  dw_uniq_cust_type_id,
  dw_uniq_port_out_operator_id,
  dw_uniq_port_req_stat_id,
  dw_uniq_port_req_stat_rsn_id,
  dw_uniq_curr_primary_dvc_cat_id,
  dw_uniq_curr_primary_dvc_id,
  dw_uniq_subs_barring_stat_id,
  dw_uniq_subs_geo_id,
  dw_uniq_subs_id,
  dw_uniq_subs_rpt_stat_id,
  dw_uniq_subs_stat_id,
  dw_uniq_subs_stat_rsn_id,
  gross_add_ind,
  is_dummy_cust,
  is_in_active_base,
  is_in_closing_base,
  is_in_contract_commitment,
  is_port_in,
  port_in_completion_dt, --NPP-19714
  port_out_completion_dt, --NPP-19714
  is_port_out,
  last_business_line_change_dt,
  last_chargeable_event_dt,
  last_rpt_tariff_plan_change_dt,
  last_subs_seg_change_dt,
  migr_of_addr_ind,
  migr_of_tech_ind,
  reconnection_ind,
  retention_ind,
  rpt_dt,
  subs_barring_dt,
  subs_conn_num,
  subs_conn_rpt_dt,
  subs_dconn_rpt_dt,
  subs_last_retention_dt,
  subs_preactivation_rpt_dt,
  subs_reconnection_rpt_dt,
  subs_rgstn_dt,
  subs_seg_change_ind,
  subs_agmt_last_term_dt,
  tariff_plan_change_ind,
  dw_uniq_physical_line_id,
  dw_curr_fixed_line_type_id,
  dw_uniq_curr_fixed_line_type_id,
  dw_prev_fixed_line_type_id,
  inactive_to_active_change_dt,
  active_to_inactive_change_dt,
  last_fixed_line_type_change_dt,
  -- v9 changes BEGIN
  dw_prev_primary_dvc_id,
  dw_acq_business_line_id,
  dw_acq_subs_seg_id,
  dw_acq_rpt_tariff_plan_id,
  dw_dconn_business_line_id,
  dw_dconn_subs_seg_id,
  dw_dconn_rpt_tariff_plan_id,
  dw_dconn_stat_rsn_id,
  last_primary_dvc_change_dt,
  primary_dvc_change_ind,
  -- v9 changes end
  ss_cd --v12 changes
FROM
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_gross_add_pstag`
  where rpt_dt = x
UNION ALL
SELECT
  business_line_change_ind,
  business_line_lvl_2_cd,
  dconn_ind,
  subs_seg_lvl_1_cd,
  dw_acq_sf_position_id,
  dw_acq_sf_type_id,
  dw_curr_business_line_id,
  dw_curr_rpt_tariff_plan_id,
  dw_curr_subs_seg_id,
  dw_cust_acct_geo_id,
  dw_cust_acct_id,
  dw_cust_geo_id,
  dw_cust_id,
  dw_cust_sector_id,
  dw_cust_seg_id,
  dw_cust_type_id,
  dw_first_acquired_prod_id,
  dw_last_retention_sf_position_id,
  dw_last_retention_sf_type_id,
  dw_physical_line_id,
  dw_port_in_operator_id,
  dw_port_out_operator_id,
  dw_port_req_stat_id,
  dw_port_req_stat_rsn_id,
  dw_prev_business_line_id,
  dw_prev_rpt_tariff_plan_id,
  dw_prev_subs_seg_id,
  dw_curr_primary_dvc_cat_id,
  dw_curr_primary_dvc_id,
  dw_sub_first_acquired_prod_id,
  dw_subs_barring_stat_id,
  dw_subs_geo_id,
  dw_subs_id,
  dw_subs_rpt_stat_id,
  dw_subs_stat_id,
  dw_subs_stat_rsn_id,
  dw_uniq_current_business_line_id,
  dw_uniq_current_rpt_tariff_plan_id,
  dw_uniq_current_subs_seg_id,
  dw_uniq_cust_acct_geo_id,
  dw_uniq_cust_acct_id,
  dw_uniq_cust_geo_id,
  dw_uniq_cust_id,
  dw_uniq_cust_sector_id,
  dw_uniq_cust_seg_id,
  dw_uniq_cust_type_id,
  dw_uniq_port_out_operator_id,
  dw_uniq_port_req_stat_id,
  dw_uniq_port_req_stat_rsn_id,
  dw_uniq_curr_primary_dvc_cat_id,
  dw_uniq_curr_primary_dvc_id,
  dw_uniq_subs_barring_stat_id,
  dw_uniq_subs_geo_id,
  dw_uniq_subs_id,
  dw_uniq_subs_rpt_stat_id,
  dw_uniq_subs_stat_id,
  dw_uniq_subs_stat_rsn_id,
  gross_add_ind,
  is_dummy_cust,
  is_in_active_base,
  is_in_closing_base,
  is_in_contract_commitment,
  is_port_in,
  port_in_completion_dt, --NPP-19714
  port_out_completion_dt, --NPP-19714
  is_port_out,
  last_business_line_change_dt,
  last_chargeable_event_dt,
  last_rpt_tariff_plan_change_dt,
  last_subs_seg_change_dt,
  migr_of_addr_ind,
  migr_of_tech_ind,
  reconnection_ind,
  retention_ind,
  rpt_dt,
  subs_barring_dt,
  subs_conn_num,
  subs_conn_rpt_dt,
  subs_dconn_rpt_dt,
  subs_last_retention_dt,
  subs_preactivation_rpt_dt,
  subs_reconnection_rpt_dt,
  subs_rgstn_dt,
  subs_seg_change_ind,
  subs_agmt_last_term_dt,
  tariff_plan_change_ind,
  dw_uniq_physical_line_id,
  dw_curr_fixed_line_type_id,
  dw_uniq_curr_fixed_line_type_id,
  dw_prev_fixed_line_type_id,
  inactive_to_active_change_dt,
  active_to_inactive_change_dt,
  last_fixed_line_type_change_dt,
  -- v9 changes BEGIN
  dw_prev_primary_dvc_id,
  dw_acq_business_line_id,
  dw_acq_subs_seg_id,
  dw_acq_rpt_tariff_plan_id,
  dw_dconn_business_line_id,
  dw_dconn_subs_seg_id,
  dw_dconn_rpt_tariff_plan_id,
  dw_dconn_stat_rsn_id,
  last_primary_dvc_change_dt,
  primary_dvc_change_ind,
  -- v9 changes end
  ss_cd -- v12 changes
FROM
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_history_from_tgt_pstag`
WHERE
  chk = 1
  OR (is_data_in_tgt = 1 AND chk IS NULL)
/* chk=1: this filter will select only those records for which history is available in target from f_subsbasesemd_history_from_tgt_stag,
 since this staging table has all gross_add<>1 records initially
chk is null:  to not filter records from hist from target table whose history is not found in tgt even though tgt is having prev data */

UNION ALL
SELECT
  business_line_change_ind,
  business_line_lvl_2_cd,
  dconn_ind,
  subs_seg_lvl_1_cd,
  dw_acq_sf_position_id,
  dw_acq_sf_type_id,
  dw_curr_business_line_id,
  dw_curr_rpt_tariff_plan_id,
  dw_curr_subs_seg_id,
  dw_cust_acct_geo_id,
  dw_cust_acct_id,
  dw_cust_geo_id,
  dw_cust_id,
  dw_cust_sector_id,
  dw_cust_seg_id,
  dw_cust_type_id,
  dw_first_acquired_prod_id,
  dw_last_retention_sf_position_id,
  dw_last_retention_sf_type_id,
  dw_physical_line_id,
  dw_port_in_operator_id,
  dw_port_out_operator_id,
  dw_port_req_stat_id,
  dw_port_req_stat_rsn_id,
  dw_prev_business_line_id,
  dw_prev_rpt_tariff_plan_id,
  dw_prev_subs_seg_id,
  dw_curr_primary_dvc_cat_id,
  dw_curr_primary_dvc_id,
  dw_sub_first_acquired_prod_id,
  dw_subs_barring_stat_id,
  dw_subs_geo_id,
  dw_subs_id,
  dw_subs_rpt_stat_id,
  dw_subs_stat_id,
  dw_subs_stat_rsn_id,
  dw_uniq_current_business_line_id,
  dw_uniq_current_rpt_tariff_plan_id,
  dw_uniq_current_subs_seg_id,
  dw_uniq_cust_acct_geo_id,
  dw_uniq_cust_acct_id,
  dw_uniq_cust_geo_id,
  dw_uniq_cust_id,
  dw_uniq_cust_sector_id,
  dw_uniq_cust_seg_id,
  dw_uniq_cust_type_id,
  dw_uniq_port_out_operator_id,
  dw_uniq_port_req_stat_id,
  dw_uniq_port_req_stat_rsn_id,
  dw_uniq_curr_primary_dvc_cat_id,
  dw_uniq_curr_primary_dvc_id,
  dw_uniq_subs_barring_stat_id,
  dw_uniq_subs_geo_id,
  dw_uniq_subs_id,
  dw_uniq_subs_rpt_stat_id,
  dw_uniq_subs_stat_id,
  dw_uniq_subs_stat_rsn_id,
  gross_add_ind,
  is_dummy_cust,
  is_in_active_base,
  is_in_closing_base,
  is_in_contract_commitment,
  is_port_in,
  port_in_completion_dt, --NPP-19714
  port_out_completion_dt, --NPP-19714
  is_port_out,
  last_business_line_change_dt,
  last_chargeable_event_dt,
  last_rpt_tariff_plan_change_dt,
  last_subs_seg_change_dt,
  migr_of_addr_ind,
  migr_of_tech_ind,
  reconnection_ind,
  retention_ind,
  rpt_dt,
  subs_barring_dt,
  subs_conn_num,
  subs_conn_rpt_dt,
  subs_dconn_rpt_dt,
  subs_last_retention_dt,
  subs_preactivation_rpt_dt,
  subs_reconnection_rpt_dt,
  subs_rgstn_dt,
  subs_seg_change_ind,
  subs_agmt_last_term_dt,
  tariff_plan_change_ind,
  dw_uniq_physical_line_id,
  dw_curr_fixed_line_type_id,
  dw_uniq_curr_fixed_line_type_id,
  dw_prev_fixed_line_type_id,
  inactive_to_active_change_dt,
  active_to_inactive_change_dt,
  last_fixed_line_type_change_dt,
  -- v9 changes BEGIN
  dw_prev_primary_dvc_id,
  dw_acq_business_line_id,
  dw_acq_subs_seg_id,
  dw_acq_rpt_tariff_plan_id,
  dw_dconn_business_line_id,
  dw_dconn_subs_seg_id,
  dw_dconn_rpt_tariff_plan_id,
  dw_dconn_stat_rsn_id,
  last_primary_dvc_change_dt,
  primary_dvc_change_ind,
  -- v9 changes end
  ss_cd --v12 changes
FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_history_from_src_pstag` ;

 ------------------- END : COMBINE GROSS_ADD, HIST FROM TARGET,HISTOR FROM SOURCE------------------------

  -----************************BEGINN**************************--------
  -----------------------ENRICH HISTORY SCENARIO-------------------------------

CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_hist_pstag` PARTITION BY rpt_dt AS
(

SELECT
  dconn_ind,
  dw_curr_business_line_id,
  dw_curr_rpt_tariff_plan_id,
  dw_curr_subs_seg_id,
  dw_cust_acct_geo_id,
  dw_cust_acct_id,
  dw_cust_geo_id,
  dw_cust_id,
  dw_cust_sector_id,
  dw_cust_seg_id,
  dw_cust_type_id,
  dw_physical_line_id,
  dw_port_in_operator_id,
  dw_port_out_operator_id,
  dw_port_req_stat_id,
  dw_port_req_stat_rsn_id,
  dw_curr_primary_dvc_cat_id,
  dw_curr_primary_dvc_id,
  dw_subs_barring_stat_id,
  dw_subs_geo_id,
  target3.dw_subs_id,
  dw_subs_rpt_stat_id,
  dw_subs_stat_id,
  target3.dw_subs_stat_rsn_id,
  dw_uniq_current_business_line_id,
  dw_uniq_current_rpt_tariff_plan_id,
  dw_uniq_current_subs_seg_id,
  dw_uniq_cust_acct_geo_id,
  dw_uniq_cust_acct_id,
  dw_uniq_cust_geo_id,
  dw_uniq_cust_id,
  dw_uniq_cust_sector_id,
  dw_uniq_cust_seg_id,
  dw_uniq_cust_type_id,
  dw_uniq_port_out_operator_id,
  dw_uniq_port_req_stat_id,
  dw_uniq_port_req_stat_rsn_id,
  dw_uniq_curr_primary_dvc_cat_id,
  dw_uniq_curr_primary_dvc_id,
  dw_uniq_subs_barring_stat_id,
  dw_uniq_subs_geo_id,
  dw_uniq_subs_id,
  dw_uniq_subs_rpt_stat_id,
  dw_uniq_subs_stat_id,
  target3.dw_uniq_subs_stat_rsn_id,
  gross_add_ind,
  is_dummy_cust,
  is_in_active_base,
  is_in_closing_base,
  is_in_contract_commitment,
  CASE WHEN target3.port_out_completion_dt <= target3.rpt_dt THEN 1 ELSE 0 END AS is_port_out, --NPP-19714
  last_chargeable_event_dt,
  migr_of_addr_ind,
  migr_of_tech_ind,
  reconnection_ind,
  target3.rpt_dt,
  subs_barring_dt,
  subs_conn_num,
  subs_conn_rpt_dt,
  subs_dconn_rpt_dt,
  subs_preactivation_rpt_dt,
  subs_reconnection_rpt_dt,
  subs_rgstn_dt,
  subs_agmt_last_term_dt,
  dw_last_retention_sf_position_id,
  dw_last_retention_sf_type_id,
  subs_last_retention_dt,
  business_line_change_ind,
  tariff_plan_change_ind,
  subs_seg_change_ind,
  dw_prev_business_line_id,
  last_business_line_change_dt,
  dw_prev_rpt_tariff_plan_id,
  last_rpt_tariff_plan_change_dt,
  dw_prev_subs_seg_id,
  last_subs_seg_change_dt,
  dw_acq_sf_position_id,
  dw_acq_sf_type_id,
  dw_first_acquired_prod_id,
  dw_sub_first_acquired_prod_id,
  CASE WHEN target3.port_in_completion_dt <= target3.rpt_dt THEN 1 ELSE 0 END AS is_port_in, --NPP-19714
  port_in_completion_dt, --NPP-19714
  port_out_completion_dt, --NPP-19714
  case when target3.last_business_line_change_dt = target3.rpt_dt
            AND target3.dw_prev_business_line_id <> to_hex(sha256("*NA"))
            and UPPER(target3.business_line_lvl_2_cd) IN UNNEST(v_bl_contract)
            AND UPPER(bl6.business_line_lvl_2_cd) IN UNNEST(v_bl_prepaid)
        then 1
        else retention_ind
		end  as retention_ind , --NPP-28106
  bl6.dw_uniq_business_line_id AS dw_uniq_prev_business_line_id,
  tp1.dw_uniq_tariff_plan_id AS dw_uniq_prev_rpt_tariff_plan_id,
  sseg2.dw_uniq_subs_seg_id AS dw_uniq_prev_subs_seg_id,
  sfp3.dw_uniq_sf_position_id AS dw_uniq_acq_sf_position_id,
  sft1.dw_uniq_sf_type_id AS dw_uniq_acq_sf_type_id,
  sfp4.dw_uniq_sf_position_id AS dw_uniq_last_retention_sf_position_id,
  sft2.dw_uniq_sf_type_id AS dw_uniq_last_retention_sf_type_id,
  prod0.dw_uniq_prod_id AS dw_uniq_first_acquired_prod_id,
  o1.dw_uniq_operator_id AS dw_uniq_port_in_operator_id,
  CASE
    WHEN target3.last_business_line_change_dt = target3.rpt_dt
    AND target3.dw_prev_business_line_id <> to_hex(sha256("*NA")) --V11 CHANGES
    --AND target3.gross_add_ind = 0 AND target3.reconnection_ind = 0 AND target3.dconn_ind = 0  --Changed logic on the migration indicator to exclude scenarios where GA/Dconn/Reconn are 1
    AND UPPER(target3.business_line_lvl_2_cd) IN UNNEST(v_bl_contract)
    AND UPPER(bl6.business_line_lvl_2_cd) IN UNNEST(v_bl_prepaid)
    THEN 1
    ELSE 0
  END AS prepay_to_contract_ind,
  CASE
    WHEN target3.last_business_line_change_dt = target3.rpt_dt
    AND target3.dw_prev_business_line_id <> to_hex(sha256("*NA")) --V11 CHANGES
    --AND target3.gross_add_ind = 0 AND target3.reconnection_ind = 0 AND target3.dconn_ind = 0 --Changed logic on the migration indicator to exclude scenarios where GA/Dconn/Reconn are 1
    AND UPPER(target3.business_line_lvl_2_cd) IN UNNEST(v_bl_prepaid)
    AND UPPER(bl6.business_line_lvl_2_cd) IN UNNEST(v_bl_contract)
    THEN 1
    ELSE 0
  END AS contract_to_prepay_ind,
  CASE
    WHEN target3.last_subs_seg_change_dt = target3.rpt_dt
    AND target3.dw_prev_subs_seg_id <> to_hex(sha256("*NA")) --V11 CHANGES
    --AND target3.gross_add_ind = 0 AND target3.reconnection_ind = 0 AND target3.dconn_ind = 0 --Changed logic on the migration indicator to exclude scenarios where GA/Dconn/Reconn are 1
    AND UPPER(target3.subs_seg_lvl_1_cd) IN UNNEST(v_subs_seg_business)
    AND UPPER(sseg2.subs_seg_lvl_1_cd) IN UNNEST(v_sub_seg_consumer)
    THEN 1
    ELSE 0
  END
  AS consumer_to_business_ind,
  CASE
    WHEN target3.last_subs_seg_change_dt = target3.rpt_dt
    AND target3.dw_prev_subs_seg_id <> to_hex(sha256("*NA")) --V11 CHANGES
    --AND target3.gross_add_ind = 0 AND target3.reconnection_ind = 0 AND target3.dconn_ind = 0 --Changed logic on the migration indicator to exclude scenarios where GA/Dconn/Reconn are 1
    AND UPPER(target3.subs_seg_lvl_1_cd) IN UNNEST(v_sub_seg_consumer)
    AND UPPER(sseg2.subs_seg_lvl_1_cd) IN UNNEST(v_subs_seg_business)
    THEN 1
    ELSE 0
  END AS business_to_consumer_ind,
  dw_uniq_physical_line_id,
  dw_curr_fixed_line_type_id,
  dw_uniq_curr_fixed_line_type_id,
  dw_prev_fixed_line_type_id,
  inactive_to_active_change_dt,
  active_to_inactive_change_dt,
  last_fixed_line_type_change_dt,
  flt1.dw_uniq_fixed_line_type_id AS dw_uniq_prev_fixed_line_type_id,
  -- v9 changes
  dw_prev_primary_dvc_id,
  dw_acq_business_line_id,
  dw_acq_subs_seg_id,
  dw_acq_rpt_tariff_plan_id,
  dw_dconn_business_line_id,
  dw_dconn_subs_seg_id,
  dw_dconn_rpt_tariff_plan_id,
  dw_dconn_stat_rsn_id,
  last_primary_dvc_change_dt,
  primary_dvc_change_ind,
  d1.dw_uniq_dvc_id AS dw_uniq_prev_primary_dvc_id,
  dca1.dw_dvc_cat_id AS dw_prev_primary_dvc_cat_id,
  dc1_dw_uniq_dvc_cat_id AS dw_uniq_prev_primary_dvc_cat_id,
  bl8.dw_uniq_business_line_id AS dw_uniq_acq_business_line_id,
  sseg3.dw_uniq_subs_seg_id AS dw_uniq_acq_subs_seg_id,
  tp2.dw_uniq_tariff_plan_id AS dw_uniq_acq_rpt_tariff_plan_id,
  bl9.dw_uniq_business_line_id AS dw_uniq_dconn_business_line_id,
  sseg4.dw_uniq_subs_seg_id AS dw_uniq_dconn_subs_seg_id,
  tp3.dw_uniq_tariff_plan_id AS dw_uniq_dconn_rpt_tariff_plan_id,
  ssr1.dw_uniq_subs_stat_rsn_id AS dw_uniq_dconn_stat_rsn_id,
  -- v9 changes
  target3.ss_cd --v12 changes
FROM
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_combined_pstag` target3

INNER JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_bl_pstag` bl6
ON
  IFNULL(target3.dw_prev_business_line_id,TO_HEX(SHA256("*NA"))) = bl6.dw_business_line_id
  /*###AND target3.rpt_dt = bl6.calendar_day_dt*/
  AND bl6.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(bl6.start_dttm) AND DATE(bl6.end_dttm) --###
INNER JOIN
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_tp_pstag` tp1
ON
  IFNULL(target3.dw_prev_rpt_tariff_plan_id,TO_HEX(SHA256("*NA"))) = tp1.dw_tariff_plan_id
  /*###AND target3.rpt_dt = tp1.calendar_day_dt*/
  AND tp1.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(tp1.start_dttm) AND DATE(tp1.end_dttm) --###
INNER JOIN
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_subseg_pstag` sseg2
ON
  IFNULL(target3.dw_prev_subs_seg_id,TO_HEX(SHA256("*NA"))) = sseg2.dw_subs_seg_id
  /*###AND target3.rpt_dt = sseg2.calendar_day_dt*/
  AND sseg2.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(sseg2.start_dttm) AND DATE(sseg2.end_dttm) --###
LEFT JOIN
  `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_product` prod0
ON
  prod0.dw_prod_id = IFNULL(target3.dw_first_acquired_prod_id,TO_HEX(SHA256("*NA")))
  and target3.rpt_dt BETWEEN DATE(prod0.start_dttm)
  AND DATE(prod0.end_dttm)

LEFT JOIN -- workaround as left join, originally it is inner join
 ( select * from `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_operator`
 where
   x BETWEEN DATE(start_dttm) AND DATE(end_dttm) and end_dttm is not null) o1
ON
  o1.dw_operator_id= IFNULL(target3.dw_port_in_operator_id,TO_HEX(SHA256("*NA")))
LEFT JOIN -- workaround as left join due to sales force potential CR, originally it is inner join
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_sf_pos_pstag` sfp3
  ON
  sfp3.dw_sf_position_id = IFNULL(target3.dw_acq_sf_position_id,TO_HEX(SHA256("*NA")))
  /*###AND target3.rpt_dt = sfp3.calendar_day_dt*/
  AND sfp3.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(sfp3.start_dttm) AND DATE(sfp3.end_dttm) --###
INNER JOIN
  `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_sf_type_pstag` sft1
ON
  sft1.dw_sf_type_id = IFNULL(target3.dw_acq_sf_type_id,TO_HEX(SHA256("*NA")))
  /*###AND target3.rpt_dt = sft1.calendar_day_dt*/
  AND sft1.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(sft1.start_dttm) AND DATE(sft1.end_dttm) --###
LEFT JOIN
    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_sf_pos_pstag` sfp4
ON
  sfp4.dw_sf_position_id = IFNULL(target3.dw_last_retention_sf_position_id,TO_HEX(SHA256("*NA")))
  /*###AND target3.rpt_dt = sfp4.calendar_day_dt*/
  AND sfp4.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(sfp4.start_dttm) AND DATE(sfp4.end_dttm) --###
INNER JOIN
    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_sf_type_pstag` sft2
ON
  sft2.dw_sf_type_id = IFNULL(target3.dw_last_retention_sf_type_id,TO_HEX(SHA256("*NA")))
  /*###AND target3.rpt_dt = sft2.calendar_day_dt*/
  AND sft2.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(sft2.start_dttm) AND DATE(sft2.end_dttm) --###
LEFT JOIN (
  SELECT
    dw_fixed_line_type_id,
    dw_uniq_fixed_line_type_id
  FROM
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_fixed_line_type`
  WHERE
    x BETWEEN DATE(start_dttm)
    AND DATE(end_dttm)
and end_dttm is not null    ) flt1
ON
  IFNULL(target3.dw_prev_fixed_line_type_id,TO_HEX(SHA256("*NA"))) = flt1.dw_fixed_line_type_id
-- v9 changes BEGIN
LEFT JOIN (
  SELECT
    dw_dvc_id,
    dw_uniq_dvc_id
  FROM
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_device`
  WHERE
    x BETWEEN DATE(start_dttm)
    AND DATE(end_dttm)
and end_dttm is not null    ) d1
ON
  IFNULL(target3.dw_prev_primary_dvc_id,TO_HEX(SHA256("*NA"))) = d1.dw_dvc_id

LEFT JOIN (
SELECT
    dca.dw_dvc_id,
    dca.dw_dvc_cat_id,
    dc1.dw_uniq_dvc_cat_id as dc1_dw_uniq_dvc_cat_id
FROM
(
  SELECT
    dw_dvc_id,
    dw_dvc_cat_id
  FROM
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_device_category_association`
  WHERE
    x BETWEEN DATE(start_dttm)
    AND DATE(end_dttm) and end_dttm is not null
) dca
LEFT JOIN (
  SELECT
    dw_dvc_cat_id,
    dw_uniq_dvc_cat_id
  FROM
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_device_category`
  WHERE
    x BETWEEN DATE(start_dttm)
    AND DATE(end_dttm) and end_dttm is not null ) dc1
ON
  dca.dw_dvc_cat_id = dc1.dw_dvc_cat_id
)dca1
ON
  IFNULL(target3.dw_prev_primary_dvc_id,TO_HEX(SHA256("*NA"))) = dca1.dw_dvc_id

LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_tp_pstag` tp2
ON
  IFNULL(target3.dw_acq_rpt_tariff_plan_id,TO_HEX(SHA256("*NA"))) = tp2.dw_tariff_plan_id
  /*###AND target3.rpt_dt = tp2.calendar_day_dt*/
  AND tp2.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(tp2.start_dttm) AND DATE(tp2.end_dttm) --###
LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_tp_pstag` tp3
ON
  IFNULL(target3.dw_dconn_rpt_tariff_plan_id,TO_HEX(SHA256("*NA"))) = tp3.dw_tariff_plan_id
  /*###AND target3.rpt_dt = tp3.calendar_day_dt*/
  AND tp3.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(tp3.start_dttm) AND DATE(tp3.end_dttm) --###
LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_bl_pstag` bl8
ON
  IFNULL(target3.dw_acq_business_line_id,TO_HEX(SHA256("*NA"))) = bl8.dw_business_line_id
  /*###AND target3.rpt_dt = bl8.calendar_day_dt*/
  AND bl8.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(bl8.start_dttm) AND DATE(bl8.end_dttm) --###
LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_bl_pstag` bl9
ON
  IFNULL(target3.dw_dconn_business_line_id,TO_HEX(SHA256("*NA"))) = bl9.dw_business_line_id
  /*###AND target3.rpt_dt = bl9.calendar_day_dt*/
  AND bl9.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(bl9.start_dttm) AND DATE(bl9.end_dttm) --###
LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_subseg_pstag` sseg3
ON
  IFNULL(target3.dw_acq_subs_seg_id,TO_HEX(SHA256("*NA"))) = sseg3.dw_subs_seg_id
  /*###AND target3.rpt_dt = sseg3.calendar_day_dt*/
  AND sseg3.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(sseg3.start_dttm) AND DATE(sseg3.end_dttm) --###
LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_subseg_pstag` sseg4
ON
  IFNULL(target3.dw_dconn_subs_seg_id,TO_HEX(SHA256("*NA"))) = sseg4.dw_subs_seg_id
  /*###AND target3.rpt_dt = sseg4.calendar_day_dt*/
  AND sseg4.end_dttm IS NOT NULL AND target3.rpt_dt BETWEEN DATE(sseg4.start_dttm) AND DATE(sseg4.end_dttm) --###
LEFT JOIN (
  SELECT
    dw_subs_stat_rsn_id,
    dw_uniq_subs_stat_rsn_id
  FROM
    `vf-pt-datahub.vfpt_dh_lake_edw_integrated_s.d_subscriber_status_reason`
  WHERE
    x BETWEEN DATE(start_dttm)
    AND DATE(end_dttm)
    and end_dttm is not null) ssr1
ON
  IFNULL(target3.dw_dconn_stat_rsn_id,TO_HEX(SHA256("*NA"))) = ssr1.dw_subs_stat_rsn_id

-- v9 changes END
--WHERE
--  target3.rpt_dt BETWEEN DATE(prod0.start_dttm)
--  AND DATE(prod0.end_dttm)

  );


  -----************************ENDD**************************----------
  -----------------------ENRICH HISTORY SCENARIO-------------------------------

  -----************************BEGINN**************************----------
  -----------------------CONVERGENCE SCENARIO-------------------------------

   CREATE OR REPLACE TABLE `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_final_pstag` PARTITION BY rpt_dt
   AS
   select eh.*,
    CASE
        WHEN  dconn_ind =1
         AND DATE(ccon0_start_dttm) = x --###v12
                THEN ccon1_dw_converge_type_id
                ELSE ccon0_dw_converge_type_id
        END AS dw_curr_converge_type_id,
    ccon1_dw_converge_type_id AS dw_prev_converge_type_id,
    CASE
        WHEN  dconn_ind =1
         AND DATE(ccon0_start_dttm) = x --###v12
                THEN ccon1_dw_uniq_converge_type_id
                ELSE ccon0_dw_uniq_converge_type_id
        END  AS dw_uniq_curr_converge_type_id,
    ccon1_dw_uniq_converge_type_id AS dw_uniq_prev_converge_type_id,
    CASE
      WHEN ccon1_converge_type_cd IS NULL
      THEN NULL
      WHEN date(ccon0_start_dttm) = x AND ccon0_converge_type_cd <> ccon1_converge_type_cd
      THEN x
      ELSE ctgt.target2_converge_change_dt
    END AS converge_change_dt,
    CASE
      WHEN ccon1_rgu_bundle_cd IS NULL
      THEN NULL
      WHEN date(ccon0_start_dttm) = x AND ccon0_rgu_bundle_cd <> ccon1_rgu_bundle_cd
      THEN x
      ELSE ctgt.target2_rgu_bundle_change_dt
    END AS rgu_bundle_change_dt,
    CASE
      WHEN ccon1_play_count IS NULL
      THEN NULL
      WHEN date(ccon0_start_dttm) = x AND ccon0_play_count <> ccon1_play_count
      THEN x
      ELSE ctgt.target2_play_count_change_dt
    END AS play_count_change_dt,
    CASE
      WHEN ccon1_converge_type_cd IS NULL
      THEN 0
      WHEN date(ccon0_start_dttm) = x AND ccon0_converge_type_cd <> ccon1_converge_type_cd
      THEN 1
      ELSE 0
    END AS converge_change_ind,
    CASE
      WHEN ccon1_rgu_bundle_cd IS NULL
      THEN 0
      WHEN date(ccon0_start_dttm) = x AND ccon0_rgu_bundle_cd <> ccon1_rgu_bundle_cd
      THEN 1
      ELSE 0
    END AS rgu_bundle_change_ind,
    CASE
      --###v12 WHEN dconn_ind = 1 then target2_play_count_change
      WHEN date(ccon0_start_dttm) = x AND ccon0_play_count = ccon1_play_count THEN v_play_count_no_change
      WHEN date(ccon0_start_dttm) = x AND SUBSTR(ccon0_play_count,0,1) ='?' THEN v_play_count_unknown
      WHEN date(ccon0_start_dttm) = x AND SUBSTR(ccon0_play_count,0,1) > SUBSTR(ccon1_play_count,0,1) THEN v_play_count_incr
      WHEN date(ccon0_start_dttm) = x AND SUBSTR(ccon0_play_count,0,1) < SUBSTR(ccon1_play_count,0,1) THEN v_play_count_decr
      WHEN date(ccon0_start_dttm) <> x THEN v_play_count_no_change
      WHEN ccon1_dw_converge_type_id IS NOT NULL THEN v_play_count_no_change
      ELSE NULL
    END AS play_count_change
   FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_enrich_hist_pstag` eh
  LEFT JOIN (
        SELECT
          *
        FROM (
          SELECT
             ccon.dw_cust_id,
            ccon.dw_converge_type_id AS ccon0_dw_converge_type_id,
            IFNULL(ccon1.dw_converge_type_id,TO_HEX(SHA256("*NA"))) AS ccon1_dw_converge_type_id,
            ccon.dw_uniq_converge_type_id AS ccon0_dw_uniq_converge_type_id,
            IFNULL(ccon1.dw_uniq_converge_type_id,TO_HEX(SHA256("*NA"))) AS ccon1_dw_uniq_converge_type_id,
            ccon.converge_type_cd AS ccon0_converge_type_cd,
            ccon1.converge_type_cd AS ccon1_converge_type_cd,
            ccon.rgu_bundle_cd AS ccon0_rgu_bundle_cd,
            ccon1.rgu_bundle_cd AS ccon1_rgu_bundle_cd,
            ccon1.play_count AS ccon1_play_count,
            ccon.play_count AS ccon0_play_count,
            ccon.start_dttm AS ccon0_start_dttm,
            ccon1.start_dttm as ccon1_start_dttm

          FROM (

              SELECT
                 cct0.dw_cust_id,
                cct0.dw_converge_type_id,
                converge_type_cd,
                rgu_bundle_cd,
                play_count,
                cct0.start_dttm,
                cct0.end_dttm,
                dw_uniq_converge_type_id
              FROM (
                        SELECT
                         dw_cust_id,
                        dw_converge_type_id,
                        start_dttm,
                        end_dttm
                        FROM
                        `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.d_customer_convergence_type`
                        WHERE
                        x>= date(start_dttm)
                        AND x <= date(end_dttm)
						QUALIFY row_number() over(partition by dw_cust_id order by dw_converge_type_id,start_dttm desc) =1
                    ) cct0
              LEFT JOIN (
                            SELECT
                             dw_converge_type_id,
                            start_dttm,
                            end_dttm,
                            converge_type_cd,
                            rgu_bundle_cd,
                            rgu_bundle_name,
                            play_count,
                            dw_uniq_converge_type_id
                            FROM
                            `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.d_convergence_type`
                            WHERE
                            x>= date(start_dttm)
                            AND x<= date(end_dttm)
                        )ccont0
              ON
                cct0.dw_converge_type_id = ccont0.dw_converge_type_id
            ) ccon
            LEFT JOIN (

                select * except(r)
                    from(SELECT
                         cct.dw_cust_id,
                        cct.start_dttm,
                        cct.end_dttm,
                        ccont1.dw_converge_type_id,
                        ccont1.start_dttm AS ccont1_start_dttm,
                        ccont1.end_dttm AS ccont1_end_dttm,
                        ccont1.converge_type_cd,
                        ccont1.rgu_bundle_cd,
                        ccont1.rgu_bundle_name,
                        ccont1.play_count,
                        ccont1.dw_uniq_converge_type_id,
                        rank() over (partition by dw_cust_id order by cct.start_dttm desc) r
                        FROM (
                        SELECT
                             dw_cust_id,
                            dw_converge_type_id,
                            start_dttm,
                            end_dttm
                        FROM
                            `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.d_customer_convergence_type`
                            where date(start_dttm) < x and date(end_dttm) </*###v12 =*/ x and date(start_dttm) <> '1900-01-01'
						QUALIFY row_number() over(partition by dw_cust_id order by dw_converge_type_id,start_dttm desc) =1
                            )cct
                        LEFT JOIN `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.d_convergence_type` ccont1
                        ON
                            cct.dw_converge_type_id = ccont1.dw_converge_type_id
                            and  cct.start_dttm between ccont1.start_dttm AND ccont1.end_dttm


                        )cct0
                            where cct0.r=1

            )ccon1
            ON
              ccon.dw_cust_id = ccon1.dw_cust_id
              and ccon.dw_converge_type_id <> IFNULL(ccon1.dw_converge_type_id,to_hex(sha256("*NA")))



        )

         )ccon0
      ON
        eh.dw_cust_id = ccon0.dw_cust_id

    LEFT JOIN
    (
        SELECT * EXCEPT (rn_by_cust_id)
        FROM
        (
        SELECT
                target2_dw_cust_id,
                target2_converge_change_dt,
                target2_rgu_bundle_change_dt,
                target2_play_count_change_dt,
                target2_play_count_change,
                row_number() over(partition by target2_dw_cust_id order by target2_rpt_dt desc) rn_by_cust_id
            FROM `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_history_from_tgt_pstag`
            where chk = 1 and
            target2_is_in_closing_base = 1
        )
    WHERE rn_by_cust_id = 1
    )ctgt
    ON  eh.dw_cust_id = ctgt.target2_dw_cust_id ;

  -----************************ENDD**************************---------------
  -----------------------CONVERGENCE SCENARIO-------------------------------

  --------------------------START : COMPLETE DATA WITH RGUs-----------------------
  -------------------FINAL INSERT INTO f_subscriber_base_semantic_d TABLE AFTER CALCULATING RGUs--------------
  --------------*******************BEGINN***************--------------------
/* In order to handle multiple insert for the smae month if the pipeline is triggered more than once, we are deleting the records first for the rpt_dt
for which the pipeline is getting executed and then insert is happening*/

DELETE
FROM
  `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_subscriber_base_semantic_d`
WHERE
  rpt_dt = x;

INSERT INTO
  `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_subscriber_base_semantic_d`
  (dw_uniq_subs_id,
    dw_subs_id,
    business_line_change_ind,
    business_to_consumer_ind,
    consumer_to_business_ind,
    contract_to_prepay_ind,
    dconn_ind,
    dw_acq_sf_position_id,
    dw_acq_sf_type_id,
    dw_curr_business_line_id,
    dw_curr_rpt_tariff_plan_id,
    dw_curr_subs_seg_id,
    dw_cust_acct_geo_id,
    dw_cust_acct_id,
    dw_cust_geo_id,
    dw_cust_id,
    dw_cust_sector_id,
    dw_cust_seg_id,
    dw_cust_type_id,
    dw_first_acquired_prod_id,
    dw_last_retention_sf_position_id,
    dw_last_retention_sf_type_id,
    dw_physical_line_id,
    dw_port_in_operator_id,
    dw_port_out_operator_id,
    dw_port_req_stat_id,
    dw_port_req_stat_rsn_id,
    dw_prev_business_line_id,
    dw_prev_rpt_tariff_plan_id,
    dw_prev_subs_seg_id,
    dw_curr_prim_dvc_cat_id,
    dw_curr_prim_dvc_id,
    dw_sub_first_acquired_prod_id,
    dw_subs_barring_stat_id,
    dw_subs_geo_id,
    dw_subs_rpt_stat_id,
    dw_subs_stat_id,
    dw_subs_stat_rsn_id,
    dw_uniq_acq_sf_position_id,
    dw_uniq_acq_sf_type_id,
    dw_uniq_curr_business_line_id,
    dw_uniq_curr_rpt_tariff_plan_id,
    dw_uniq_curr_subs_seg_id,
    dw_uniq_cust_acct_geo_id,
    dw_uniq_cust_acct_id,
    dw_uniq_cust_geo_id,
    dw_uniq_cust_id,
    dw_uniq_cust_sector_id,
    dw_uniq_cust_seg_id,
    dw_uniq_cust_type_id,
    dw_uniq_first_acquired_prod_id,
    dw_uniq_last_retention_sf_position_id,
    dw_uniq_last_retention_sf_type_id,
    dw_uniq_port_in_operator_id,
    dw_uniq_port_out_operator_id,
    dw_uniq_port_req_stat_id,
    dw_uniq_port_req_stat_rsn_id,
    dw_uniq_prev_business_line_id,
    dw_uniq_prev_rpt_tariff_plan_id,
    dw_uniq_prev_subs_seg_id,
    dw_uniq_curr_prim_dvc_cat_id,
    dw_uniq_curr_prim_dvc_id,
    dw_uniq_subs_barring_stat_id,
    dw_uniq_subs_geo_id,
    dw_uniq_subs_rpt_stat_id,
    dw_uniq_subs_stat_id,
    dw_uniq_subs_stat_rsn_id,
    gross_add_ind,
    is_dummy_cust,
    is_in_active_base,
    is_in_closing_base,
    is_in_contract_commitment,
    is_port_in,
	port_in_completion_dt, --NPP-19714
	port_out_completion_dt, --NPP-19714
    is_port_out,
    last_business_line_change_dt,
    last_chargeable_event_dt,
    last_rpt_tariff_plan_change_dt,
    last_subs_seg_change_dt,
    migr_of_addr_ind,
    migr_of_tech_ind,
    num_of_bb_rgus,
    num_of_tv_rgus,
    num_of_voice_rgus,
    prepay_to_contract_ind,
    reconnection_ind,
    retention_ind,
    rpt_dt,
    subs_barring_dt,
    subs_conn_num,
    subs_conn_rpt_dt,
    subs_dconn_rpt_dt,
    subs_last_retention_dt,
    subs_preactivation_rpt_dt,
    subs_reconnection_rpt_dt,
    subs_rgstn_dt,
    subs_seg_change_ind,
    subs_agmt_last_term_dt,
    tariff_plan_change_ind,
    dw_uniq_physical_line_id,
    dw_curr_fixed_line_type_id,
    dw_uniq_curr_fixed_line_type_id,
    dw_prev_fixed_line_type_id,
    dw_curr_converge_type_id,
    dw_prev_converge_type_id,
    dw_uniq_prev_converge_type_id,
    dw_uniq_curr_converge_type_id,
    converge_change_dt,
    rgu_bundle_change_dt,
    play_count_change_dt,
    inactive_to_active_change_dt,
    active_to_inactive_change_dt,
    converge_change_ind,
    rgu_bundle_change_ind,
    play_count_change,
	ss_cd, --v12 changes
    last_fixed_line_type_change_dt,
    dw_uniq_prev_fixed_line_type_id,
    -- v9 changes BEGIN
    dw_prev_prim_dvc_id,
    dw_uniq_prev_prim_dvc_id,
    dw_prev_prim_dvc_cat_id,
    dw_uniq_prev_prim_dvc_cat_id,
    dw_acq_business_line_id,
    dw_uniq_acq_business_line_id,
    dw_acq_subs_seg_id,
    dw_uniq_acq_subs_seg_id,
    dw_acq_rpt_tariff_plan_id,
    dw_uniq_acq_rpt_tariff_plan_id,
    dw_dconn_business_line_id,
    dw_uniq_dconn_business_line_id,
    dw_dconn_subs_seg_id,
    dw_uniq_dconn_subs_seg_id,
    dw_dconn_rpt_tariff_plan_id,
    dw_uniq_dconn_rpt_tariff_plan_id,
    dw_dconn_stat_rsn_id,
    dw_uniq_dconn_stat_rsn_id,
    last_prim_dvc_change_dt,
    prim_dvc_change_ind,  -- v9 changes end
    extraction_dttm,
    load_dttm,
    update_dttm,
    insert_load_id,
    update_load_id )
SELECT
  DISTINCT ifnull(dw_uniq_subs_id,to_hex(sha256("*NA"))),
  ifnull(dw_subs_id,to_hex(sha256("*NA"))),
  ifnull(business_line_change_ind,0),
  ifnull(business_to_consumer_ind,0),
  ifnull(consumer_to_business_ind,0),
  ifnull(contract_to_prepay_ind,0),
  dconn_ind,
  ifnull(dw_acq_sf_position_id,to_hex(sha256("*NA"))),
  ifnull(dw_acq_sf_type_id,to_hex(sha256("*NA"))),
  ifnull(dw_curr_business_line_id,to_hex(sha256("*NA"))),
  ifnull(dw_curr_rpt_tariff_plan_id,to_hex(sha256("*NA"))),
  ifnull(dw_curr_subs_seg_id,to_hex(sha256("*NA"))),
  ifnull(dw_cust_acct_geo_id,to_hex(sha256("*NA"))),
  ifnull(dw_cust_acct_id,to_hex(sha256("*NA"))),
  ifnull(dw_cust_geo_id,to_hex(sha256("*NA"))),
  ifnull(dw_cust_id,to_hex(sha256("*NA"))),
  ifnull(dw_cust_sector_id,to_hex(sha256("*NA"))),
  ifnull(dw_cust_seg_id,to_hex(sha256("*NA"))),
  ifnull(dw_cust_type_id,to_hex(sha256("*NA"))),
  ifnull(dw_first_acquired_prod_id,to_hex(sha256("*NA"))),
  ifnull(dw_last_retention_sf_position_id,to_hex(sha256("*NA"))),
  ifnull(dw_last_retention_sf_type_id,to_hex(sha256("*NA"))),
  ifnull(dw_physical_line_id,to_hex(sha256("*NA"))),
  ifnull(dw_port_in_operator_id,to_hex(sha256("*NA"))),
  ifnull(dw_port_out_operator_id,to_hex(sha256("*NA"))),
  ifnull(dw_port_req_stat_id,to_hex(sha256("*NA"))),
  ifnull(dw_port_req_stat_rsn_id,to_hex(sha256("*NA"))),
  ifnull(dw_prev_business_line_id,to_hex(sha256("*NA"))),
  ifnull(dw_prev_rpt_tariff_plan_id,to_hex(sha256("*NA"))),
  ifnull(dw_prev_subs_seg_id,to_hex(sha256("*NA"))),
  ifnull(dw_curr_primary_dvc_cat_id,to_hex(sha256("*NA"))),
  ifnull(dw_curr_primary_dvc_id,to_hex(sha256("*NA"))),
  ifnull(dw_sub_first_acquired_prod_id,to_hex(sha256("*NA"))),
  ifnull(dw_subs_barring_stat_id,to_hex(sha256("*NA"))),
  ifnull(dw_subs_geo_id,to_hex(sha256("*NA"))),
  ifnull(dw_subs_rpt_stat_id,to_hex(sha256("*NA"))),
  ifnull(dw_subs_stat_id,to_hex(sha256("*NA"))),
  ifnull(dw_subs_stat_rsn_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_acq_sf_position_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_acq_sf_type_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_current_business_line_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_current_rpt_tariff_plan_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_current_subs_seg_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_cust_acct_geo_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_cust_acct_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_cust_geo_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_cust_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_cust_sector_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_cust_seg_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_cust_type_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_first_acquired_prod_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_last_retention_sf_position_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_last_retention_sf_type_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_port_in_operator_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_port_out_operator_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_port_req_stat_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_port_req_stat_rsn_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_prev_business_line_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_prev_rpt_tariff_plan_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_prev_subs_seg_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_curr_primary_dvc_cat_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_curr_primary_dvc_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_subs_barring_stat_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_subs_geo_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_subs_rpt_stat_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_subs_stat_id,to_hex(sha256("*NA"))),
  ifnull(dw_uniq_subs_stat_rsn_id,to_hex(sha256("*NA"))),
  gross_add_ind,
  is_dummy_cust,
  ifnull(is_in_active_base,0),
  is_in_closing_base,
  ifnull(is_in_contract_commitment,0),
  ifnull(is_port_in,0),
  port_in_completion_dt, --NPP-19714
  port_out_completion_dt, --NPP-19714
  ifnull(is_port_out,0),
  last_business_line_change_dt,
  last_chargeable_event_dt,
  last_rpt_tariff_plan_change_dt,
  last_subs_seg_change_dt,
  migr_of_addr_ind,
  migr_of_tech_ind,
  COUNTIF(
  IF
    ((UPPER(business_line_lvl_3_cd) IN UNNEST(v_business_line_rgu_mobile_bb)
        OR UPPER(business_line_lvl_3_cd) IN UNNEST(v_business_line_rgu_fix_bb)),
      dw_sub_subs_id,
      NULL) IS NOT NULL) OVER (PARTITION BY dw_subs_id) AS num_of_broadband_rgus,  --Group by subscriber. This grouping is used to count numbers of RGUs.
  COUNTIF(
  IF
    ((UPPER(business_line_lvl_3_cd) IN UNNEST(v_business_line_rgu_mobile_tv)
        OR UPPER(business_line_lvl_3_cd) IN UNNEST(v_business_line_rgu_fix_tv)),
      dw_sub_subs_id,
      NULL) IS NOT NULL) OVER (PARTITION BY dw_subs_id) AS num_of_tv_rgus,  --Group by subscriber. This grouping is used to count numbers of RGUs.
  COUNTIF(
  IF
    ((UPPER(business_line_lvl_3_cd) IN UNNEST(v_business_line_rgu_mobile_voice)
        OR UPPER(business_line_lvl_3_cd) IN UNNEST(v_business_line_rgu_fix_voice)),
      dw_sub_subs_id,
      NULL) IS NOT NULL) OVER (PARTITION BY dw_subs_id) AS num_of_voice_rgus,  --Group by subscriber. This grouping is used to count numbers of RGUs.
  ifnull(prepay_to_contract_ind,0) as prepay_to_contract_ind,
  ifnull(reconnection_ind,0) as reconnection_ind,
  ifnull(retention_ind,0) as retention_ind,
  rpt_dt,
  subs_barring_dt,
  ifnull(subs_conn_num,"*NA"),
  ifnull(subs_conn_rpt_dt,parse_date("%Y%m%d",'19000101')),
  subs_dconn_rpt_dt,
  subs_last_retention_dt,
  ifnull(subs_preactivation_rpt_dt,parse_date("%Y%m%d",'19000101')),
  subs_reconnection_rpt_dt,
  subs_rgstn_dt,
  ifnull(subs_seg_change_ind,0),
  subs_agmt_last_term_dt,
  ifnull(tariff_plan_change_ind,0),
  IFNULL(dw_uniq_physical_line_id,to_hex(sha256("*NA"))),
  IFNULL(dw_curr_fixed_line_type_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_curr_fixed_line_type_id,to_hex(sha256("*NA"))),
  IFNULL(dw_prev_fixed_line_type_id,to_hex(sha256("*NA"))),
  IFNULL(dw_curr_converge_type_id,to_hex(sha256("*NA"))),
  IFNULL(dw_prev_converge_type_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_prev_converge_type_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_curr_converge_type_id,to_hex(sha256("*NA"))),
  converge_change_dt,
  rgu_bundle_change_dt,
  play_count_change_dt,
  inactive_to_active_change_dt,
  active_to_inactive_change_dt,
  converge_change_ind,
  rgu_bundle_change_ind,
  play_count_change,
  ss_cd, --v12 changes
  last_fixed_line_type_change_dt,
  IFNULL(dw_uniq_prev_fixed_line_type_id,to_hex(sha256("*NA"))),
  -- v9 changes BEGIN
  IFNULL(dw_prev_primary_dvc_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_prev_primary_dvc_id,to_hex(sha256("*NA"))),
  IFNULL(dw_prev_primary_dvc_cat_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_prev_primary_dvc_cat_id,to_hex(sha256("*NA"))),
  IFNULL(dw_acq_business_line_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_acq_business_line_id,to_hex(sha256("*NA"))),
  IFNULL(dw_acq_subs_seg_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_acq_subs_seg_id,to_hex(sha256("*NA"))),
  IFNULL(dw_acq_rpt_tariff_plan_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_acq_rpt_tariff_plan_id,to_hex(sha256("*NA"))),
  IFNULL(dw_dconn_business_line_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_dconn_business_line_id,to_hex(sha256("*NA"))),
  IFNULL(dw_dconn_subs_seg_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_dconn_subs_seg_id,to_hex(sha256("*NA"))),
  IFNULL(dw_dconn_rpt_tariff_plan_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_dconn_rpt_tariff_plan_id,to_hex(sha256("*NA"))),
  IFNULL(dw_dconn_stat_rsn_id,to_hex(sha256("*NA"))),
  IFNULL(dw_uniq_dconn_stat_rsn_id,to_hex(sha256("*NA"))),
  last_primary_dvc_change_dt,
  primary_dvc_change_ind, -- V9 CHANGES END
  TIMESTAMP_TRUNC(current_timestamp, SECOND) AS extraction_dttm,
  TIMESTAMP_TRUNC(current_timestamp, SECOND) AS load_dttm,
  TIMESTAMP_TRUNC(current_timestamp, SECOND) AS update_dttm,
  (CAST(SUBSTR(REGEXP_REPLACE(CAST(current_timestamp AS STRING), r'[\-\:\ \\d]+',''),0,14) AS INT64)) AS insert_load_id,
  (CAST(SUBSTR(REGEXP_REPLACE(CAST(current_timestamp AS STRING), r'[\-\:\ \\d]+',''),0,14) AS INT64)) AS update_load_id
FROM (
  SELECT
    target4.*,
    s6.dw_sub_subs_id,
    s6.business_line_lvl_2_cd,
    s6.business_line_lvl_3_cd
  FROM
    `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_final_pstag` target4
  LEFT JOIN (  -- To Calculate RGUs--
    SELECT
      dw_subs_id,
      dw_sub_subs_id,
      business_line_lvl_2_cd,
      business_line_lvl_3_cd,
      start_dttm,
      end_dttm,
      /*###calendar_day_dt*/
    FROM
      `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_f_subsbasesemd_d_subscriber_main_pstg`
    WHERE
      (UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_active)
        --v12 ### OR ( UPPER(subs_rpt_stat_cd) IN UNNEST(v_sub_stat_inactive)
        --v12 ###  AND subs_dconn_rpt_dt = DATE(start_dttm)
        --v12 ###  AND DATE(start_dttm) = x )
          )
      AND UPPER(business_line_lvl_1_cd) IN UNNEST(v_bl_rgu)
      AND end_dttm IS NOT NULL AND x BETWEEN DATE(start_dttm) AND DATE(end_dttm) --###
      /*###AND calendar_day_dt = x*/

        )s6
  ON
    s6.dw_subs_id = target4.dw_subs_id
    /*###and s6.calendar_day_dt = target4.rpt_dt) ;*/
    AND x = target4.rpt_dt); --###

   INSERT INTO `vf-pt-datahub.vfpt_dh_lake_edw_staging_s.gs_tmp_hist_subs_base_sem_d_and_m_pstag`
      SELECT  dw_subs_id
             ,dw_curr_business_line_id
             ,dw_prev_business_line_id
             ,subs_agmt_last_term_dt
             ,last_business_line_change_dt
             ,dw_curr_rpt_tariff_plan_id
             ,dw_prev_rpt_tariff_plan_id
             ,last_rpt_tariff_plan_change_dt
             ,dw_curr_subs_seg_id
             ,dw_prev_subs_seg_id
             ,last_subs_seg_change_dt
             ,dw_acq_sf_position_id
             ,dw_acq_sf_type_id
             ,dw_last_retention_sf_position_id
             ,dw_last_retention_sf_type_id
             ,dw_first_acquired_prod_id
             ,dw_sub_first_acquired_prod_id
             ,is_port_in
			 ,port_in_completion_dt --NPP-19714
			 ,port_out_completion_dt --NPP-19714
             ,dw_port_in_operator_id
             ,subs_last_retention_dt
             ,dw_curr_fixed_line_type_id
             ,dw_prev_fixed_line_type_id
             ,last_fixed_line_type_change_dt
             ,converge_change_dt
             ,rgu_bundle_change_dt
             ,play_count_change_dt
             ,dw_cust_id
             ,rpt_dt
             ,play_count_change
             ,is_in_closing_base
             ,is_in_active_base
             ,inactive_to_active_change_dt
             ,dw_curr_prim_dvc_id
             ,dw_prev_prim_dvc_id
             ,dw_acq_business_line_id
             ,dw_acq_rpt_tariff_plan_id
             ,dw_acq_subs_seg_id
             ,dw_dconn_business_line_id
             ,dw_dconn_subs_seg_id
             ,dw_dconn_rpt_tariff_plan_id
             ,dw_dconn_stat_rsn_id
             ,last_prim_dvc_change_dt
        FROM `vf-pt-datahub.vfpt_dh_lake_edw_reporting_s.f_subscriber_base_semantic_d`
       WHERE rpt_dt = x;

SET
  x=DATE_ADD(x,INTERVAL 1 DAY);
END --###
;