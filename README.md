# google_sheet_report
# This script demonstrates how to connect to BigQuery using Python,
# fetch and merge data from multiple tables, and export the final result to Google Sheets.

import datetime as dt
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd
import warnings
import pandas_gbq
from google.oauth2 import service_account

warnings.simplefilter("ignore")
datestring_d0 = dt.datetime.strftime(dt.datetime.now() - dt.timedelta(days=8), '%Y-%m-%d')
aaj_ke_date = dt.datetime.strftime(dt.datetime.now() - dt.timedelta(days=0), '%Y-%m-%d')
current_hour = dt.datetime.strftime(dt.datetime.now() - dt.timedelta(days=0), '%H')
kal = dt.datetime.strftime(dt.datetime.now() - dt.timedelta(days=1), '%Y-%m-%d')

current_date = dt.datetime.strftime(dt.datetime.now() - dt.timedelta(days=0), '%Y_%m_%d_%H_%M')

#home_directory="C:/auth/"
home_directory='/home/'
#home_directory = 'C:/Users/admin/Documents/magicpin_jobs/manifests/tele_sales/Json/'

# need add your 
bq_auth_file = f'{home_directory}magicpin-analytics-growth-team.json'
gsheet_auth = f'{home_directory}gsheet_creds.json'

#bq_auth_file = r"c:\Users\Admin\Desktop\auth\magicpin_analytics_growth_team.json"
#gsheet_auth = r"c:\Users\Admin\Desktop\auth\gsheet_creds.json"

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(gsheet_auth, scope)
gc = gspread.authorize(credentials)

def upload_to_bq(faltu):
    import pandas_gbq
    from google.oauth2 import service_account
    pd.set_option("display.max_columns",100)
    project_id_bq = 'magicpin-14cba'
    credentials_bq = service_account.Credentials.from_service_account_file("/home/analytics/venv/Analymis_bq_json_file.json")
    client_bq = bigquery.Client(credentials = credentials_bq, project = project_id_bq)
    bq_auth_file = '/home/analytics/venvals/mis_bq_json_file.json'
    credentials = service_account.Credentials.from_service_account_file(bq_auth_file)
    pandas_gbq.to_gbq(faltu, ####$%^&*() place')#For First Time replace




ws = gc.open_by_url('https://docs.google.com/spreadsheets/d/1lFDJmjzAGxpWKUnjuGr5RRnCEP26Xpa8BhGQkew2IHA/edit#gid=0').worksheet('Sheet15')
df = pd.DataFrame(ws.get_all_records())

df2 = df[['MID']]
print(df2)
df2 = df2[~df2['MID'].isna()]
df1 = tuple(df2['MID'].astype(int).to_list())
print(df1)


def big_qu_df(query):
    credentials_bq = service_account.Credentials.from_service_account_file(bq_auth_file)
    rdf = pandas_gbq.read_gbq(query, project_id='magicpin-14cba', credentials=credentials_bq)
    return rdf


sql1 = f"""
    SELECT m.id as MID, m.merchant_name, md.is_automatic_on_allowed, m.parent_merchant_id, m.locality, m.locality_id, pc.is_shadowed,
   date(pc.shadow_date) as delisted_date, pc.shadow_remark, date(pc.unshadow_date) as relisting_date,
   m.enable_order_booking, m.accepting_delivery, pc.magicpay_app_shadowed, m.highlight1, pc.voucher_commission,
   ifnull(CAST(SPLIT(SPLIT(pc.magicpay_day_wise_mfd_commission, ',')[safe_OFFSET(1)], ':')[safe_OFFSET(1)] AS float64),
     merchant_sponsored_usable_balance_percent) AS magicpay_mfd,
   ifnull(CAST(SPLIT(SPLIT(pc.magicorder_day_wise_mfd_commission, ',')[safe_OFFSET(1)], ':')[safe_OFFSET(1)] AS float64),
          merchant_sponsored_usable_balance_percent) AS magicorder_mfd, pc.delivery_commission,ifnull(ifnull(cast(split(split(magicorder_day_wise_mfd,",")[safe_offset(1)],":")[safe_offset(1)] as float64),merchant_sponsored_usable_balance_percent),0) A,
 merchant_sponsored_usable_balance_percent AS mfd, merchant_sponsored_discount_commission as mfd_c,
 (ifnull(pc.merchant_sponsored_usable_balance_percent,
 0) + pc.magicpay_usable_balance_percent) AS savepercent ,pc.is_shadowed as is_listed, 
 ifnull(cast(split(split(magicorder_day_wise_mfd,",")[safe_offset(1)],":")[safe_offset(1)] as float64),merchant_sponsored_usable_balance_percent) as magicorder_mfd,
 ifnull(CAST(SPLIT(SPLIT(magicpay_day_wise_mfd_commission,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
 merchant_sponsored_usable_balance_percent) AS magicpay_mfd,
 ifnull(CAST(SPLIT(SPLIT(cash_voucher_day_wise_mfd_commission,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
 merchant_sponsored_discount_commission) AS cashvoucher_mfd,
 ifnull(CAST(SPLIT(SPLIT(combo_voucher_day_wise_mfd_commission,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
 merchant_sponsored_usable_balance_percent) AS combovoucher_mfd,
ifnull( ifnull(cast(split(split(magicorder_day_wise_mfd,",")[safe_offset(1)],":")[safe_offset(1)] as float64),merchant_sponsored_usable_balance_percent),0) + magicorder_usable_balance_percent as delivery_save,
 ifnull( ifnull(CAST(SPLIT(SPLIT(magicpay_day_wise_mfd,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
 merchant_sponsored_usable_balance_percent),0) + magicpay_usable_balance_percent as magicpay_save,
ifnull( ifnull(CAST(SPLIT(SPLIT(cash_voucher_day_wise_mfd,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
merchant_sponsored_usable_balance_percent),0) + redemption_usable_balance_percent as voucher_save, pc.updated_at,minimum_order_value,max_magicorder_mfd

    FROM `magicpin-14cba.wallet_latest.merchant` m
    LEFT JOIN `wallet_latest.merchant_delivery_info` md ON md.merchant_id = m.id
    LEFT JOIN `magicpin-14cba.wallet_latest.payment_commercials` pc ON pc.merchant_user_id = m.user_id
    WHERE commercials_type = "MARKETING" AND m.id IN {str(df1)};
"""
sql_query1 = big_qu_df(sql1)
print(sql_query1)
sql_query1['MID'] = sql_query1['MID'].astype(int)


sql2 = f"""
select MID, 1 as Voucher_listed from      (SELECT m.parent_merchant_id as PID, m.id as MID,      m.city,m.locality,m.merchant_name as Mx_Name, m.user_id as      Mx_User_Id, case when m.highlight5 ='parent' then 1 else 0 end as PID_CHECK,case when magicpay_app_shadowed=1 or (magicpay_enabled = 0 or magicpay_enabled IS NULL)      then 0 else 1 end as magicpay_status ,case when m.enable_order_booking is null or m.enable_order_booking = 0      then 0 else 1 end as magicorder ,count(distinct(case when roa.is_active = 1 and roa.is_app_shadowed = 0 and      (roa.is_hidden = 0 or roa.is_hidden is null) then roa.id end)) as voucher_count FROM      ( select supportedMerchants as userid,updated_at AS UR, id AS ROID, is_shadowed, tags AS ROTags,      voucher_country AS country1, category_tags, supportedmerchants, activation_date, deactivation_date,      is_active from (SELECT  * except(c) REPLACE(c as supportedMerchants) FROM `magicpin-14cba.wallet_latest.redemption_option` ,      UNNEST(SPLIT(supportedMerchants))c where supportedMerchants IS NOT NULL   AND is_active = 1 AND is_local_merchant=1      and supportedMerchants != '')x group by 1,2,3,4,5,6,7,8,9,10,11 union all SELECT distinct merchantuserid, ro.updated_at as UR,      id AS ROID,is_shadowed, CASE WHEN LEFT(tags, 4) in ('2019','2020','2021','2022') THEN tags ELSE 'XYZ' END AS ROTags,voucher_country      as country1, category_tags,supportedmerchants,activation_date, deactivation_date,is_active FROM      `magicpin-14cba.wallet_latest.redemption_option`  ro WHERE is_active = 1 AND merchantuserid IS NOT NULL )x      LEFT JOIN `magicpin-14cba.wallet_latest.merchant` m ON x.userid = cast(m.user_id as string)      left join `magicpin-14cba.wallet_latest.redemption_option_amount` roa on roa.option_id=x.ROID      left join `magicpin-14cba.wallet_latest.redemption_option_partner` rop on      rop.option_amount_id = roa.id left join `magicpin-14cba.wallet_latest.payment_commercials`      pc on cast(pc.merchant_user_id as string) = x.userid WHERE m.id in {str(df1)} and m.is_shadowed=0      and m.city NOT IN ('Bekasi','Jakarta','Depok','Tangerang') and pc.is_shadowed = 0 and      (merchant_priority is null or merchant_priority =0) and pc.commercials_type = 'MARKETING'      group by 1,2,3,4,5,6,7,8,9 having PID_CHECK = 0 and (magicpay_status = 1 OR magicorder = 1 or voucher_count > 0))      where voucher_count >0; 
"""
#sql_query2 = pd.read_gbq(sql2, project_id='magicpin-analytics', credentials=credentials, dialect='standard')
sql_query2 = big_qu_df(sql2)
print(sql_query2)


sql3 = f"""
SELECT
    merchant_id AS MID,
    SUM(ctm) AS Last_90D_CTM,
    COUNT(DISTINCT txn_id) AS Last_90D_Transaction,
    COUNT(CASE WHEN DATE(redemption_date) = CURRENT_DATE() - 1 and ctm!=0 THEN txn_id END) AS Last_1_Txn,
    COUNT(CASE WHEN DATE(redemption_date) = CURRENT_DATE() - 7 and ctm!=0 THEN txn_id END) AS Last_D7_Txn,
    COUNT(CASE WHEN DATE(redemption_date) >= CURRENT_DATE() - 7 and ctm!=0 THEN txn_id END) AS Running_D7_Txn,
    COUNT(DISTINCT CASE WHEN DATE(redemption_date) >= CURRENT_DATE() - 30 and ctm!=0 THEN txn_id END) AS Last_D30_txn,
    SUM(CASE WHEN DATE(redemption_date) = CURRENT_DATE() - 1 and ctm!=0 THEN ctm END) AS D1_CTM,
    SUM(CASE WHEN DATE(redemption_date) = CURRENT_DATE() - 7 and ctm!=0 THEN ctm END) AS D7_CTM,
    SUM(CASE WHEN DATE(redemption_date) >= CURRENT_DATE() - 7 and ctm!=0 THEN ctm END) AS Running_D7_CTM,
    SUM(CASE WHEN DATE(redemption_date) >= CURRENT_DATE() - 30 and ctm!=0 THEN ctm END) AS D30_CTM,
    --Delivery--
    COUNT(CASE WHEN DATE(redemption_date) = CURRENT_DATE() - 1 and ctm!=0 and lower(product_type) in ('delivery')  THEN txn_id END) AS Online_D1_Txn,
    COUNT(CASE WHEN DATE(redemption_date) = CURRENT_DATE() - 7 and ctm!=0 and lower(product_type) in ('delivery') THEN txn_id END) AS Online_D7_Txn,
    COUNT(CASE WHEN DATE(redemption_date) >= CURRENT_DATE() - 30 and ctm!=0 and lower(product_type) in ('delivery') THEN txn_id END) AS Online_D30_Txn
from `txn_level_cm_reporting.txn_level_cm_reporting_main`
where lower(product_type) in ('delivery','magicpay','voucher') and
date(redemption_date) >= current_date()-interval 90 day and merchant_id in {str(df1)}
group by 1;
"""
sql_query3 = pd.DataFrame(big_qu_df(sql3))
print(len(sql_query3))
sql_query3['MID'] = sql_query3['MID'].astype(int)


sql_cancel = f"""
SELECT
  merchant_id as MID,
  count(case when (date(paid_at)) = current_date() - 1 then (date(paid_at)) end) as Cancel_D1,
  count(case when (date(paid_at)) = current_date()  then (date(paid_at)) end) as Cancel_D0,
  count(case when (date(paid_at)) = current_date() - 7 then (date(paid_at)) end) as Cancel_D7,
  count(case when (date(paid_at)) = current_date() - 30 then (date(paid_at)) end) as Cancel_D30
FROM
  `magicpin-14cba.delivery_analytics_as.orders_analytics`
WHERE
  merchant_id IN {str(df1)} 
  AND order_status = 'CANCELLED'
GROUP BY
  merchant_id;
"""
sql_query_cancel = pd.DataFrame(big_qu_df(sql_cancel))
print(len(sql_query_cancel))


sql5 = f"""
SELECT
    m.id AS MID,
    SUM(amount) AS Last_90D_amount,
    COUNT((CASE WHEN DATE(wr.created_at) = CURRENT_DATE()  THEN wr.id END)) AS D0Txn,
    COUNT(CASE WHEN DATE(wr.created_at) = CURRENT_DATE() AND LOWER(product_type) IN ('magicpay') THEN wr.id END) AS Offline_D0_magicpay,
    COUNT(CASE WHEN DATE(wr.created_at) = CURRENT_DATE()-7 AND LOWER(product_type) IN ('magicpay') THEN wr.id END) AS Offline_D7_magicpay,
    COUNT(CASE WHEN DATE(wr.created_at) = CURRENT_DATE()-1 AND LOWER(product_type) IN ('magicpay') THEN wr.id END) AS Offline_D1_magicpay,
    COUNT(CASE WHEN DATE(wr.created_at) >= CURRENT_DATE()-30 AND LOWER(product_type) IN ('magicpay') THEN wr.id END) AS Offline_D30_magicpay,
    COUNT( distinct CASE WHEN DATE(wr.created_at)=current_date() AND LOWER(product_type) IN ('magicpay') then wr.target_user_id end) as user_D0, 
    COUNT( distinct CASE WHEN DATE(wr.created_at)=current_date()-1 AND LOWER(product_type) IN ('magicpay') then wr.target_user_id end) as user_D1,
    COUNT( distinct CASE WHEN DATE(wr.created_at)=current_date()-7 AND LOWER(product_type) IN ('magicpay') then wr.target_user_id end) as user_D7,
    SUM(CASE WHEN DATE(wr.created_at) = CURRENT_DATE() THEN wr.amount END) AS GrossD0,
    SUM(CASE WHEN DATE(wr.created_at) = CURRENT_DATE() AND LOWER(product_type) IN ('magicpay') THEN wr.amount END) AS Offline_D0_magicpayCTM,
    SUM(CASE WHEN DATE(wr.created_at) = CURRENT_DATE()-7 AND LOWER(product_type) IN ('magicpay') THEN wr.amount END) AS Offline_D7_magicpayCTM
 FROM
     `magicpin-14cba.wallet_latest.wallet_redemption` wr
left join `magicpin-14cba.wallet_latest.merchant` m on m.user_id=wr.merchant_user_id
WHERE
    LOWER(product_type) IN ('delivery', 'magicpay', 'voucher')
    AND DATE(wr.created_at) >= CURRENT_DATE() - INTERVAL 90 DAY and m.id in {str(df1)}
group by 1;
"""
sql_query5 = pd.DataFrame(big_qu_df(sql5))
print(len(sql_query5))
sql_query5['MID'] = sql_query5['MID'].astype(int)


final_data = pd.merge(sql_query1, sql_query2, on='MID', how='outer')
final_data = pd.merge(final_data, sql_query3, on='MID', how='outer')
final_data = pd.merge(final_data, sql_query_cancel, on='MID', how='outer')
final_data = pd.merge(final_data, sql_query5, on='MID', how='outer')

final_data = final_data.drop_duplicates(subset='MID')

# sht = gc.open_by_url('https://docs.google.com/spreadsheets/d/1lFDJmjzAGxpWKUnjuGr5RRnCEP26Xpa8BhGQkew2IHA/edit#gid=1823117422').worksheet('Shubh')
# gd.set_with_dataframe(sht, final_data, resize=True, row=1, col=1)


