{% macro most_recent_record(relation, pk_fields) %}

{% set partition_by = pk_fields|join(',') %}

(
	select * from (

		select 
			*,
			max(_sdc_sequence) over (partition by {{ partition_by }}) as most_recent_sequence,
			max(_sdc_batched_at) over (partition by {{ partition_by }}) as most_recent_batch
		from {{ relation }} 

	) 
	where _sdc_sequence = most_recent_sequence
	and _sdc_batched_at = most_recent_batch

)

{% endmacro %}