import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import io
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# ページ設定
st.set_page_config(page_title="広告効果測定BQツール", layout="wide")

# ============================================================
# 関数定義
# ============================================================

def create_query(anken_id_val, shop_id, item_id, easy_id_list, start_date, end_date):
    """BigQueryクエリを生成する"""
    easy_id_str = ', '.join(map(str, easy_id_list)) if easy_id_list else 'NULL'
    
    # 日付オブジェクトへの変換
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    
    # --- 日付計算ロジック ---
    # sale_end_datetime: E列 + 1日 00:00:00
    end_date_input_dt = datetime.strptime(end_date, '%Y-%m-%d')
    sale_end_date = (end_date_input_dt + timedelta(days=1)).strftime('%Y-%m-%d')
    
    # end_datetime: D列(開始日) の1ヶ月後 00:00:00
    end_date_plus1month = (start_date_dt + relativedelta(months=1)).strftime('%Y-%m-%d')
    
    # クエリ定義
    query = f"""
-- 変数宣言
DECLARE shopid, itemid INT64;
DECLARE easyid ARRAY<INT64>;
DECLARE start_datetime, end_datetime, sale_start_datetime, sale_end_datetime datetime;

-- パラメータ設定
SET shopid = {shop_id};
SET itemid = {item_id};
SET easyid = [{easy_id_str}];
SET start_datetime = '{start_date} 00:00:00';
SET end_datetime = '{end_date_plus1month} 00:00:00';
SET sale_start_datetime = '{start_date} 00:00:00';
SET sale_end_datetime = '{sale_end_date} 00:00:00';

-- 集計
WITH shop_purchaser_tbl AS (
SELECT distinct easy_id
FROM `spdb-data.ua_view_mk_ichiba.red_basket_detail_tbl` x
WHERE reg_datetime >= DATE_ADD(start_datetime,INTERVAL -365 DAY)
  AND reg_datetime < start_datetime
  AND (cancel_datetime IS NULL OR cancel_datetime >= start_datetime)
  AND NOT EXISTS(SELECT *  FROM `spdb-data.ua_view_mk_ichiba.illegal_order` y  WHERE x.order_no = y.order_number)
  AND shop_id = shopid
)

SELECT distinct a.easy_id,
       a.fullname,
       (case when Sale_GMS is null then 0 else Sale_GMS end) as Sale_GMS,
       (case when Sale_GMS_ROOM is null then 0 else Sale_GMS_ROOM end) as Sale_GMS_ROOM,
       (case when Monthly_GMS is null then 0 else Monthly_GMS end) as Monthly_GMS,
       (case when Monthly_GMS_ROOM is null then 0 else Monthly_GMS_ROOM end) as Monthly_GMS_ROOM,

       (case when Sale_Order is null then 0 else Sale_Order end) as Sale_Order,
       (case when Sale_Order_ROOM is null then 0 else Sale_Order_ROOM end) as Sale_Order_ROOM,
       (case when Monthly_Order is null then 0 else Monthly_Order end) as Monthly_Order,
       (case when Monthly_Order_ROOM is null then 0 else Monthly_Order_ROOM end) as Monthly_Order_ROOM,

       (case when Sale_Purchaser is null then 0 else Sale_Purchaser end) as Sale_Purchaser,
       (case when Sale_Purchaser_ROOM is null then 0 else Sale_Purchaser_ROOM end) as Sale_Purchaser_ROOM,
       (case when Monthly_Purchaser is null then 0 else Monthly_Purchaser end) as Monthly_Purchaser,
       (case when Monthly_Purchaser_ROOM is null then 0 else Monthly_Purchaser_ROOM end) as Monthly_Purchaser_ROOM,

       (case when Sale_Purchaser_New is null then 0 else Sale_Purchaser_New end) as Sale_Purchaser_New,
       (case when Sale_Purchaser_ROOM_New is null then 0 else Sale_Purchaser_ROOM_New end) as Sale_Purchaser_ROOM_New,
       (case when Monthly_Purchaser_New is null then 0 else Monthly_Purchaser_New end) as Monthly_Purchaser_New,
       (case when Monthly_Purchaser_ROOM_New is null then 0 else Monthly_Purchaser_ROOM_New end) as Monthly_Purchaser_ROOM_New,

       (case when Sale_Item_GMS is null then 0 else Sale_Item_GMS end) as Sale_Item_GMS,
       (case when Sale_Item_GMS_ROOM is null then 0 else Sale_Item_GMS_ROOM end) as Sale_Item_GMS_ROOM,
       (case when Monthly_Item_GMS is null then 0 else Monthly_Item_GMS end) as Monthly_Item_GMS,
       (case when Monthly_Item_GMS_ROOM is null then 0 else Monthly_Item_GMS_ROOM end) as Monthly_Item_GMS_ROOM,
       
       (case when Sale_Click is null then 0 else Sale_Click end) as Sale_Click,
       (case when Sale_Click_ROOM is null then 0 else Sale_Click_ROOM end) as Sale_Click_ROOM,
       (case when Monthly_Click is null then 0 else Monthly_Click end) as Monthly_Click,
       (case when Monthly_Click_ROOM is null then 0 else Monthly_Click_ROOM end) as Monthly_Click_ROOM,

       (case when Sale_Item_Click is null then 0 else Sale_Item_Click end) as Sale_Item_Click,
       (case when Sale_Item_Click_ROOM is null then 0 else Sale_Item_Click_ROOM end) as Sale_Item_Click_ROOM,
       (case when Monthly_Item_Click is null then 0 else Monthly_Item_Click end) as Monthly_Item_Click,
       (case when Monthly_Item_Click_ROOM is null then 0 else Monthly_Item_Click_ROOM end) as Monthly_Item_Click_ROOM

FROM (
  SELECT distinct easy_id, fullname
  FROM (SELECT easy_id, fullname, ROW_NUMBER() OVER (PARTITION BY easy_id ORDER BY time_stamp DESC) rn
        FROM `spdb-data.ua_view_mk_room.user_tbl`)
  where rn = 1 and
        easy_id in UNNEST(easyid)
) a

LEFT JOIN (
  SELECT distinct af_id,
          sum(case when dt >= cast(sale_start_datetime as date) and dt < cast(sale_end_datetime as date) then click_num else 0 end) as Sale_Click,
          sum(case when dt >= cast(sale_start_datetime as date) and dt < cast(sale_end_datetime as date) and pointback_prefix = '_RTroom' then click_num else 0 end) as Sale_Click_ROOM,
          sum(click_num) as Monthly_Click,
          sum(case when pointback_prefix = '_RTroom' then click_num else 0 end) as Monthly_Click_ROOM,

          sum(case when dt >= cast(sale_start_datetime as date) and dt < cast(sale_end_datetime as date) and item_id = itemid then click_num else 0 end) as Sale_Item_Click,
          sum(case when dt >= cast(sale_start_datetime as date) and dt < cast(sale_end_datetime as date) and pointback_prefix = '_RTroom'  and item_id = itemid then click_num else 0 end) as Sale_Item_Click_ROOM,
          sum(case when item_id = itemid then click_num else 0 end) as Monthly_Item_Click,
          sum(case when pointback_prefix = '_RTroom' and item_id = itemid then click_num else 0 end) as Monthly_Item_Click_ROOM

  FROM `spdb-data.ua_view_mk_afl.click_log_summary`
  WHERE af_id in UNNEST(easyid) and
        shop_id = shopid and
        dt >= cast(start_datetime as date) and
        dt < cast(end_datetime as date)
  GROUP BY 1
  ) b
ON a.easy_id = b.af_id

LEFT JOIN (
  SELECT distinct easy_id,
          sum(case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime then (unit_price * quantity) else 0 end) as Sale_GMS,
          sum(case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and log_pointback like '_RTroom%' then (unit_price * quantity) else 0 end) as Sale_GMS_ROOM,
          sum((unit_price * quantity)) as Monthly_GMS,
          sum(case when log_pointback like '_RTroom%' then (unit_price * quantity) else 0 end) as Monthly_GMS_ROOM,

          count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime then log_oid else null end) as Sale_Order,
          count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and log_pointback like '_RTroom%' then log_oid else null end) as Sale_Order_ROOM,
          count(distinct log_oid) as Monthly_Order,
          count(distinct case when log_pointback like '_RTroom%' then log_oid else null end) as Monthly_Order_ROOM,

          count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and purchase_user_id > 0 then purchase_user_id else null end) as Sale_Purchaser,
          count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and purchase_user_id > 0 and log_pointback like '_RTroom%' then purchase_user_id else null end) as Sale_Purchaser_ROOM,
          count(distinct case when purchase_user_id > 0 then purchase_user_id else null end) as Monthly_Purchaser,
          count(distinct case when purchase_user_id > 0 and log_pointback like '_RTroom%' then purchase_user_id else null end) as Monthly_Purchaser_ROOM,

          count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and purchase_user_id > 0 and purchase_user_id not in (select * from shop_purchaser_tbl) then purchase_user_id else null end) as Sale_Purchaser_New,
          count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and purchase_user_id > 0 and purchase_user_id not in (select * from shop_purchaser_tbl) and log_pointback like '_RTroom%' then purchase_user_id else null end) as Sale_Purchaser_ROOM_New,
          count(distinct case when purchase_user_id > 0 and purchase_user_id not in (select * from shop_purchaser_tbl) then purchase_user_id else null end) as Monthly_Purchaser_New,
          count(distinct case when purchase_user_id > 0 and purchase_user_id not in (select * from shop_purchaser_tbl) and log_pointback like '_RTroom%' then purchase_user_id else null end) as Monthly_Purchaser_ROOM_New,

          sum(case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and item_id = itemid then (unit_price * quantity) else 0 end) as Sale_Item_GMS,
          sum(case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and log_pointback like '_RTroom%' and item_id = itemid then (unit_price * quantity) else 0 end) as Sale_Item_GMS_ROOM,
          sum(case when item_id = itemid then (unit_price * quantity) else 0 end) as Monthly_Item_GMS,
          sum(case when log_pointback like '_RTroom%' and item_id = itemid then (unit_price * quantity) else 0 end) as Monthly_Item_GMS_ROOM

  FROM `spdb-data.ua_view_mk_afl.result_goods_n`
  WHERE easy_id in UNNEST(easyid) and
        me_id = (1000000 + shopid) and
        me_id = link_me_id and
        log_resulttime >= start_datetime and
        log_resulttime < end_datetime and
        log_clicktime >= start_datetime and
        log_clicktime < end_datetime
  GROUP BY 1
) c
ON a.easy_id = c.easy_id
ORDER BY 3 desc;
"""
    return query

def process_single_group(client, group_data, passthrough_cols):
    """1つのグループに対する処理"""
    try:
        idx, group_key, group = group_data
        anken_id, anken_name, shop_id, item_id, start_date, end_date = group_key
        
        easy_id_list = group['easy_id'].unique().tolist()
        
        if not easy_id_list:
            return None

        start_date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
        
        # クエリ作成
        query = create_query(
            anken_id_val=anken_id, 
            shop_id=shop_id,
            item_id=item_id,
            easy_id_list=easy_id_list,
            start_date=start_date_str,
            end_date=end_date_str
        )
        
        # クエリ実行
        query_job = client.query(query)
        result_df = query_job.result().to_dataframe()
        
        if len(result_df) > 0:
            # 基本情報の付与
            if 'AnkenID' in result_df.columns:
                result_df = result_df.rename(columns={'AnkenID': '案件ID'})
            else:
                result_df['案件ID'] = anken_id 
            
            result_df['案件名'] = anken_name
            result_df['shop_id'] = shop_id
            result_df['item_id'] = item_id
            result_df['紹介開始日'] = start_date_str
            result_df['紹介終了日'] = end_date_str
            
            # 自動転記ロジック
            reward_amount = 0
            for col in passthrough_cols:
                val = group[col].iloc[0]
                result_df[col] = val
                
                if col == '確定報酬金額':
                    reward_amount = pd.to_numeric(val, errors='coerce')

            # ROAS計算ロジック
            if pd.notna(reward_amount) and reward_amount > 0:
                result_df['個別ROAS'] = ((result_df['Sale_GMS'] / reward_amount) * 100).fillna(0).astype(int).astype(str) + '%'
                result_df['個別Monthly ROAS'] = ((result_df['Monthly_GMS'] / reward_amount) * 100).fillna(0).astype(int).astype(str) + '%'
            else:
                result_df['個別ROAS'] = "0%"
                result_df['個別Monthly ROAS'] = "0%"
            
            return result_df
        else:
            return None

    except Exception as e:
        # エラー発生時はエラー情報を返す
        return {"error": str(e), "anken_id": anken_id}

# ============================================================
# メイン処理 (Streamlit UI)
# ============================================================

st.title("広告効果測定 BQ実行ツール")
st.markdown("""
Excelファイルをアップロードすると、BigQueryを実行して効果測定結果を集計します。
""")

# サイドバー設定
st.sidebar.header("設定")
PROJECT_ID = st.sidebar.text_input("Project ID", value="spdb-cm-cc-ichiba2")
MAX_WORKERS = st.sidebar.number_input("並列実行数", min_value=1, max_value=32, value=8)

# ファイルアップロード
uploaded_file = st.file_uploader("インポート用Excelファイルをアップロード", type=["xlsx"])

if uploaded_file is not None:
    # データ読み込み
    try:
        df_input = pd.read_excel(uploaded_file)
        st.success(f"ファイル読み込み成功: {len(df_input)} 行")
    except Exception as e:
        st.error(f"ファイル読み込みエラー: {e}")
        st.stop()

    # 実行ボタン
    if st.button("集計開始"):
        # BigQueryクライアント初期化
        try:
            if "gcp_service_account_json" in st.secrets:
                json_str = st.secrets["gcp_service_account_json"]
                key_dict = json.loads(json_str)
                creds = service_account.Credentials.from_service_account_info(key_dict)
                client = bigquery.Client(credentials=creds, project=PROJECT_ID)
            else:
                client = bigquery.Client(project=PROJECT_ID)
                st.info("ローカル認証情報を使用しています。")
        except Exception as e:
            st.error(f"BigQuery接続エラー: {e}")
            st.info("Secrets設定を確認してください。")
            st.stop()

        progress_bar = st.progress(0)
        status_text = st.empty()
        error_logs = []

        # --- 前処理 ---
        status_text.text("データを前処理中...")
        
        df_input.columns = df_input.columns.str.strip()
        column_rename_map = {
            'EasyID': 'easy_id', 'shopID': 'shop_id', 'itemID': 'item_id',
            'SNS紹介開始日': '紹介開始日', 'SNS紹介終了日': '紹介終了日'
        }
        actual_rename_map = {k: v for k, v in column_rename_map.items() if k in df_input.columns}
        df_input = df_input.rename(columns=actual_rename_map)

        if '案件ID' in df_input.columns:
            df_input['案件ID'] = df_input['案件ID'].astype(str).str.strip()

        grouping_keys = ['案件ID', '案件名', 'shop_id', 'item_id', '紹介開始日', '紹介終了日']
        exclude_keys = ['easy_id']
        passthrough_cols = [c for c in df_input.columns if c not in grouping_keys and c not in exclude_keys]
        
        grouped = df_input.groupby(grouping_keys)
        total_groups = len(grouped)
        st.info(f"対象グループ数: {total_groups}")

        # --- 並列処理実行 ---
        all_results = []
        group_data_list = [(i, k, v) for i, (k, v) in enumerate(grouped, 1)]
        
        completed_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_single_group, client, data, passthrough_cols) for data in group_data_list]
            
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    if isinstance(res, dict) and "error" in res:
                        # エラー情報を収集
                        error_logs.append(f"案件ID: {res['anken_id']} - Error: {res['error']}")
                    else:
                        all_results.append(res)
                
                completed_count += 1
                progress = completed_count / total_groups
                progress_bar.progress(progress)
                status_text.text(f"処理中... {completed_count}/{total_groups} 完了")

        # --- 結果結合と出力 ---
        if error_logs:
            st.error("以下のエラーが発生しました:")
            for log in error_logs:
                st.write(log)

        if not all_results:
            st.warning("有効な結果が0件でした。")
        else:
            status_text.text("結果ファイルを作成中...")
            df_final = pd.concat(all_results, ignore_index=True)

            fixed_headers = ['案件ID', '案件名', 'shop_id', 'item_id', '紹介開始日', '紹介終了日']
            user_headers = ['easy_id', 'fullname']
            metric_headers = [c for c in df_final.columns if c not in fixed_headers and c not in passthrough_cols and c not in user_headers and c not in ['個別ROAS', '個別Monthly ROAS']]
            
            final_order = fixed_headers + passthrough_cols + user_headers + metric_headers + ['個別ROAS', '個別Monthly ROAS']
            final_order = [c for c in final_order if c in df_final.columns]
            
            df_final = df_final[final_order]

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_final.to_excel(writer, index=False)
            
            buffer.seek(0)
            
            st.success("集計完了！以下のボタンからダウンロードしてください。")
            st.download_button(
                label="結果Excelをダウンロード",
                data=buffer,
                file_name=f"効果測定結果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
