{%- macro fill(dataset, table, client, desired_columns) -%}

  {%- set actual_dataset = dataset + '_' + client -%}
    {{ log("Actual dataset: " ~ actual_dataset, info=true) }}

  {%- set actual_columns = adapter.get_columns_in_table(actual_dataset, table) -%}
    {{ log("Actual columns: " ~ actual_columns, info=true) }}

  {%- set actual_column_names = actual_columns|map(attribute='name')|list -%}
    {{ log("Actual column names: " ~ actual_column_names, info=true) }}

  {%- for desired_column in desired_columns -%}
    {%- if not loop.first %}
, {% else %}
  {% endif -%}
    {%- if desired_column in actual_column_names -%}
      {{ desired_column }}
    {%- else -%}
      null as {{ desired_column }}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro -%}