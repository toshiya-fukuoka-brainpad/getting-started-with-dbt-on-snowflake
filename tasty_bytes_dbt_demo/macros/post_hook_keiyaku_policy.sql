{% macro post_hook_keiyaku_policy() %}

-- DB / Schema を target から自動取得
USE DATABASE {{ target.database }};
USE SCHEMA {{ target.schema }};

-- ① Row Access Policy 作成 (EXISTS 形式)
-- 関数を介さず、直接 T_KEIYAKU を参照します
CREATE OR REPLACE ROW ACCESS POLICY filter_by_T_KEIYAKU
AS (val_ANKEN_NO VARCHAR) RETURNS BOOLEAN ->
  CURRENT_ROLE() = 'ACCOUNTADMIN'
  OR EXISTS (
    SELECT 1 FROM T_KEIYAKU
    WHERE ANKEN_NO = val_ANKEN_NO
  );

-- ② クラスタリング（検索効率を上げるために重要）
ALTER TABLE T_ANKEN CLUSTER BY (ANKEN_NO);

-- ③ ポリシー適用
-- ※既に適用済みの場合は、一度 DROP するか、新規作成時にのみ実行する制御が必要です
ALTER TABLE T_ANKEN ADD ROW ACCESS POLICY filter_by_T_KEIYAKU ON (ANKEN_NO);

{% endmacro %}
