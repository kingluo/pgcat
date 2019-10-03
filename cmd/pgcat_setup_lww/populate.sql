do $$
declare
	v_ts_field text = 'ts:row';
	v_new_ts timestamp;
	v_sysid bigint;
	v_rowcount int;
begin
	v_new_ts = clock_timestamp();
	select system_identifier from pg_control_system() into v_sysid;

	loop
		update {{ .Table }} set __pgcat_lww =
			('{"' || v_ts_field || '": "' || v_new_ts::text || '"}')::jsonb
			{{- range .IndividualCols}}
			|| ('{"ts:{{ . }}": "' || v_new_ts::text || '"}')::jsonb
			{{- end}}
			{{- range .CounterCols}}
			|| ('{"' || 'val:{{ . }}@' || v_sysid ||
				'": ' || {{ . }} || '}')::jsonb
			{{- end}}
			|| ('{"sysid": ' || v_sysid || '}')::jsonb
			WHERE ctid IN (SELECT ctid FROM {{ .Table }}
				where __pgcat_lww is null LIMIT 1000);
		GET DIAGNOSTICS v_rowcount = ROW_COUNT;
		if v_rowcount = 0 then
			exit;
		end if;
		commit;
	end loop;
end;
$$;
