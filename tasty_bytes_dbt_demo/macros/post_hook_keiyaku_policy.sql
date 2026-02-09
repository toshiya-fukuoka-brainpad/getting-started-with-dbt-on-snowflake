{% macro post_hook_keiyaku_policy() %}

-- DB / Schema を target から自動取得
USE DATABASE {{ target.database }};
USE SCHEMA {{ target.schema }};

-- ③ 案件番号リストをキャッシュする関数
CREATE OR REPLACE FUNCTION get_keiyaku_anken_array()
RETURNS ARRAY
MEMOIZABLE
AS
$$
  SELECT ARRAY_AGG(DISTINCT ANKEN_NO)
  FROM T_KEIYAKU
$$;

-- ④ Row Access Policy 作成
CREATE OR REPLACE ROW ACCESS POLICY filter_by_T_KEIYAKU
AS (val_ANKEN_NO VARCHAR) RETURNS BOOLEAN ->
  CURRENT_ROLE() = 'ACCOUNTADMIN'
  OR ARRAY_CONTAINS(val_ANKEN_NO::VARIANT, get_keiyaku_anken_array());

-- ⑤ クラスタリング
ALTER TABLE T_ANKEN CLUSTER BY (ANKEN_NO);

-- ⑥ ポリシー適用
ALTER TABLE T_ANKEN ADD ROW ACCESS POLICY filter_by_T_KEIYAKU ON (ANKEN_NO);

{% endmacro %}
