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

  - If the statement already has a top-level COMMENT = ... clause (at paren
    depth 0, outside any string literal), returns the input unchanged --
    inline SQL wins.
  - If the statement ends with COPY GRANTS, inserts the COMMENT clause
    BEFORE it so COPY GRANTS remains the final clause (Snowflake grammar).
  - Treats '...' and $$...$$ as opaque so a literal COMMENT inside a
    description never false-matches. A regex would be simpler but cannot
    track paren depth, which we need to ignore per-dimension COMMENTs.
  - $ in comment_text is escaped to [$] (Snowflake convention) so the
    description can never close the enclosing $$...$$ block.
-#}
{% macro append_comment_if_missing(sql, comment_text) -%}
  {%- set s = (sql | trim) -%}
  {%- if s[-1:] == ';' -%}
    {%- set s = (s[:-1] | trim) -%}
  {%- endif -%}

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

  {%- set lower_body = body | lower -%}
  {%- set ns = namespace(has_comment=false) -%}

  {# Cheap bailout: skip the full scanner when no candidate token exists. #}
  {%- if 'comment' in lower_body -%}
    {%- set n = body | length -%}
    {%- set ns2 = namespace(i=0, depth=0) -%}
    {%- for _ in range(n) -%}
      {%- if ns2.i < n and not ns.has_comment -%}
        {%- set ch = body[ns2.i] -%}
        {%- if ch == "'" -%}
          {%- set ns2.i = ns2.i + 1 -%}
          {%- set closed = namespace(done=false) -%}
          {%- for _ in range(n - ns2.i) -%}
            {%- if not closed.done and ns2.i < n -%}
              {%- if body[ns2.i] == "'" -%}
                {%- if body[ns2.i + 1:ns2.i + 2] == "'" -%}
                  {%- set ns2.i = ns2.i + 2 -%}
                {%- else -%}
                  {%- set ns2.i = ns2.i + 1 -%}
                  {%- set closed.done = true -%}
                {%- endif -%}
              {%- else -%}
                {%- set ns2.i = ns2.i + 1 -%}
              {%- endif -%}
            {%- endif -%}
          {%- endfor -%}
        {%- elif ch == '$' and body[ns2.i + 1:ns2.i + 2] == '$' -%}
          {%- set ns2.i = ns2.i + 2 -%}
          {%- set closed = namespace(done=false) -%}
          {%- for _ in range(n - ns2.i) -%}
            {%- if not closed.done and ns2.i < n -%}
              {%- if body[ns2.i] == '$' and body[ns2.i + 1:ns2.i + 2] == '$' -%}
                {%- set ns2.i = ns2.i + 2 -%}
                {%- set closed.done = true -%}
              {%- else -%}
                {%- set ns2.i = ns2.i + 1 -%}
              {%- endif -%}
            {%- endif -%}
          {%- endfor -%}
        {%- elif ch == '(' -%}
          {%- set ns2.depth = ns2.depth + 1 -%}
          {%- set ns2.i = ns2.i + 1 -%}
        {%- elif ch == ')' -%}
          {%- set ns2.depth = ns2.depth - 1 -%}
          {%- set ns2.i = ns2.i + 1 -%}
        {%- elif ns2.depth == 0 and lower_body[ns2.i:ns2.i + 7] == 'comment' -%}
          {%- set after = ns2.i + 7 -%}
          {%- set prev_char = body[ns2.i - 1:ns2.i] -%}
          {%- set prev_ok = prev_char == '' or not (prev_char.isalnum() or prev_char == '_') -%}
          {%- set next_char = body[after:after + 1] -%}
          {%- set next_ok = next_char == '' or not (next_char.isalnum() or next_char == '_') -%}
          {%- if prev_ok and next_ok -%}
            {%- set rest = body[after:] | trim -%}
            {%- if rest and rest[0] == '=' -%}
              {%- set ns.has_comment = true -%}
            {%- endif -%}
          {%- endif -%}
          {%- set ns2.i = after -%}
        {%- else -%}
          {%- set ns2.i = ns2.i + 1 -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
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