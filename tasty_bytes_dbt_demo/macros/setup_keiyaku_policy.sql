{% macro setup_keiyaku_policy() %}

{# 1. 現在のデータベースとスキーマをコンテキストから取得 #}
{% set current_db = target.database %}
{% set current_schema = schema %}

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
{# ※ 既に適用されている場合にエラーにならないよう一度削除してから追加するのが安全です #}
ALTER TABLE {{ ref('T_ANKEN') }} DROP ROW ACCESS POLICY filter_by_T_KEIYAKU;
ALTER TABLE {{ ref('T_ANKEN') }} ADD ROW ACCESS POLICY {{ current_db }}.{{ current_schema }}.filter_by_T_KEIYAKU ON (ANKEN_NO);

{% endmacro %}
