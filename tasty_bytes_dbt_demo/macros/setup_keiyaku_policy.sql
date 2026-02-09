{% macro setup_keiyaku_policy() %}
{{
  config(
    meta={
      'tags': ['keiyaku', 'security']
    }
  )
}}

-- DB / Schema を target から自動取得
{% set db = target.database %}
{% set sc = target.schema %}

{% do run_query("USE DATABASE " ~ db) %}
{% do run_query("USE SCHEMA " ~ sc) %}

-- ③ 案件番号リストをキャッシュする関数
{% do run_query("""
CREATE OR REPLACE FUNCTION get_keiyaku_anken_array()
RETURNS ARRAY
MEMOIZABLE
AS
$$
  SELECT ARRAY_AGG(DISTINCT ANKEN_NO)
  FROM T_KEIYAKU
$$;
""") %}

-- ④ Row Access Policy 作成
{% do run_query("""
CREATE OR REPLACE ROW ACCESS POLICY filter_by_T_KEIYAKU
AS (val_ANKEN_NO VARCHAR) RETURNS BOOLEAN ->
  CURRENT_ROLE() = 'ACCOUNTADMIN'
  OR ARRAY_CONTAINS(val_ANKEN_NO::VARIANT, get_keiyaku_anken_array());
""") %}

-- ⑤ クラスタリング
{% do run_query("ALTER TABLE T_ANKEN CLUSTER BY (ANKEN_NO);") %}

-- ⑥ ポリシー適用
{% do run_query("ALTER TABLE T_ANKEN ADD ROW ACCESS POLICY filter_by_T_KEIYAKU ON (ANKEN_NO);") %}

{% endmacro %}
