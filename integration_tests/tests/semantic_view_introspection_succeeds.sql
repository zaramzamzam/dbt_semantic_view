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

{{ config(materialized='test') }}

{# Trigger adapter introspection on a semantic view relation
   For semantic views, our override returns 0 columns. Expect length == 0. #}
{% set relation = ref('semantic_view_basic') %}
{% set columns = adapter.get_columns_in_relation(relation) %}
{% set col_names = columns | map(attribute='name') | map('upper') | list %}

-- Fail if columns list is NOT empty (should be 0 for semantic views)
select 'semantic view columns should be empty' as error_message
where {{ (col_names | length) }} != 0



