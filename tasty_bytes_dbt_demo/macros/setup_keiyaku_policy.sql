{% macro setup_keiyaku_policy() %}

{# target.database は profiles の database (SILVER) を参照します #}
{% set current_db = target.database %}

{# target.schema は profiles の schema (CLN) を参照します #}
{# もし dbt_project.yml でスキーマを上書きしている場合は {{ schema }} が適切です #}
{% set current_schema = target.schema %}

-- デバッグ用にログを出力（dbt run 時に確認可能）
{% do log("Running policy macro on: " ~ current_db ~ "." ~ current_schema, info=True) %}

{# 2. メモイザブル関数の作成 #}
CREATE OR REPLACE FUNCTION {{ current_db }}.{{ current_schema }}.get_keiyaku_anken_array()
RETURNS ARRAY
MEMOIZABLE
AS
'SELECT ARRAY_AGG(DISTINCT ANKEN_NO) FROM {{ current_db }}.{{ current_schema }}.T_KEIYAKU';

{# 3. 行アクセスポリシーの作成 #}
CREATE OR REPLACE ROW ACCESS POLICY {{ current_db }}.{{ current_schema }}.filter_by_T_KEIYAKU
AS (val_ANKEN_NO VARCHAR) RETURNS BOOLEAN ->
  CURRENT_ROLE() = 'ACCOUNTADMIN'
  OR 
  ARRAY_CONTAINS(val_ANKEN_NO::VARIANT, {{ current_db }}.{{ current_schema }}.get_keiyaku_anken_array());

{# 4. ポリシーを T_ANKEN に適用 #}
{# refを使用することで、dbtが依存関係を解決した正しいテーブル名を自動取得します #}
ALTER TABLE {{ ref('T_ANKEN') }} DROP ROW ACCESS POLICY IF EXISTS {{ current_db }}.{{ current_schema }}.filter_by_T_KEIYAKU;
ALTER TABLE {{ ref('T_ANKEN') }} ADD ROW ACCESS POLICY {{ current_db }}.{{ current_schema }}.filter_by_T_KEIYAKU ON (ANKEN_NO);

{% endmacro %}
