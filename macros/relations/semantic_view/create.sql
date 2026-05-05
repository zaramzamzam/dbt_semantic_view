-- Copyright 2025 Snowflake Inc. 
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{% macro snowflake__get_create_semantic_view_sql(relation, sql) -%}
{#-
--  Produce DDL that creates a semantic view
--
--  Args:
--  - relation: Union[SnowflakeRelation, str]
--      - SnowflakeRelation - required for relation.render()
--      - str - is already the rendered relation name
--  - sql: str - the code defining the model
--  Returns:
--      A valid DDL statement which will result in a new semantic view.
-#}

  create or replace semantic view {{ relation }}
  {{ sql }}

{%- endmacro %}


{% macro append_copy_grants_if_missing(sql) -%}
  {%- set s = (sql | trim) -%}
  {%- set had_semicolon = (s[-1:] == ';') -%}
  {%- if had_semicolon -%}
    {%- set s = (s[:-1] | trim) -%}
  {%- endif -%}

  {# detect existing COPY GRANTS at the end, case/whitespace-insensitive #}
  {%- set tokens = s.split() -%}
  {%- set ends_with_copy = (tokens | length >= 2)
      and ((tokens[-2] | lower) == 'copy')
      and ((tokens[-1] | lower) == 'grants') -%}

  {%- if ends_with_copy -%}
    {%- set out = s -%}
  {%- else -%}
    {%- set out = s ~ '\nCOPY GRANTS' -%}
  {%- endif -%}

  {{- out -}}
{%- endmacro %}


{#-
  append_comment_if_missing: idempotently appends
    COMMENT = $$<comment_text>$$
  to a CREATE OR REPLACE SEMANTIC VIEW statement.

  - If the statement already has a top-level COMMENT = ... clause (after the
    last closing parenthesis), returns the input unchanged — inline SQL wins.
  - If the statement ends with COPY GRANTS, inserts the COMMENT clause
    BEFORE it so COPY GRANTS remains the final clause (Snowflake grammar).
  - Relation-level COMMENT can only appear after all clause bodies (after
    the last ')'), so only the text after the last ')' needs to be scanned.
    This avoids Jinja's MAX_RANGE sandbox limit on large compiled SQL bodies.
  - Known limitation: if an existing user-written COMMENT value contains a
    literal ')', rfind(')') may land inside that value and the check will
    miss it, causing a duplicate COMMENT append. This edge case is accepted
    as a regression vs. the prior character-by-character scanner. In practice
    relation-level COMMENT values rarely contain ')'.
  - $ in comment_text is escaped to [$] (Snowflake convention) so the
    description can never close the enclosing $$...$$ block.
-#}
{% macro append_comment_if_missing(sql, comment_text) -%}
  {%- set s = (sql | trim) -%}
  {%- if s[-1:] == ';' -%}{%- set s = (s[:-1] | trim) -%}{%- endif -%}

  {%- set tokens = s.split() -%}
  {%- set ends_with_copy = (tokens | length >= 2)
      and ((tokens[-2] | lower) == 'copy')
      and ((tokens[-1] | lower) == 'grants') -%}

  {%- if ends_with_copy -%}
    {%- set lower_s = s | lower -%}
    {%- set cg_idx = lower_s.rfind('copy grants') -%}
    {%- set body = (s[:cg_idx] | trim) -%}
    {%- set tail = s[cg_idx:] -%}
  {%- else -%}
    {%- set body = s -%}
    {%- set tail = '' -%}
  {%- endif -%}

  {#- A relation-level COMMENT can only appear after all clause bodies (after last ')').
      Scan only the tail after the last ')' to detect existing COMMENT. -#}
  {%- set last_paren = body.rfind(')') -%}
  {%- set ns = namespace(has_comment=false) -%}
  {%- if last_paren != -1 -%}
    {%- set after_body = body[last_paren + 1:] | lower -%}
    {%- set ns.has_comment = modules.re.search('\\bcomment\\s*=', after_body) is not none -%}
  {%- endif -%}

  {%- set escaped = comment_text | replace('$', '[$]') -%}
  {%- if ns.has_comment -%}
    {{- sql -}}
  {%- elif ends_with_copy -%}
    {{- body ~ '\nCOMMENT = $$' ~ escaped ~ '$$\n' ~ tail -}}
  {%- else -%}
    {{- body ~ '\nCOMMENT = $$' ~ escaped ~ '$$' -}}
  {%- endif -%}
{%- endmacro %}


{% macro snowflake__create_or_replace_semantic_view() %}
  {%- set identifier = model['alias'] -%}

  {%- set copy_grants = config.get('copy_grants', default=false) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='view') -%}

  {%- if config.persist_column_docs() -%}
    {%- set sql = dbt_semantic_view.append_column_comments_if_missing(sql) -%}
  {%- endif -%}

  {%- if copy_grants -%}
    {%- set sql = dbt_semantic_view.append_copy_grants_if_missing(sql) -%}
  {%- endif -%}

  {%- if config.persist_relation_docs() and model.description -%}
    {%- set sql = dbt_semantic_view.append_comment_if_missing(sql, model.description) -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks) }}

  -- build model
  {% call statement('main') -%}
    {{ dbt_semantic_view.snowflake__get_create_semantic_view_sql(target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}