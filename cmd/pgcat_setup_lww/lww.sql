-- add meta column

do $$
begin
	alter table {{ .Table }} add column __pgcat_lww jsonb;
exception when duplicate_column then
	-- do nothing
end;
$$;

-- trigger

CREATE or replace FUNCTION
{{ .Table }}_pgcat_lww_trigger() RETURNS trigger AS $$
declare
	v_new_ts timestamp with time zone;
	v_ts_field text = 'ts:row';
	v_tombstone_field text = '__deleted';
	v_is_replica boolean = false;
	v_sysid bigint;
	v_name1 text;
	v_name2 text;
	v_should_update boolean;
	v_counter text;
	v_counters text[];
	v_pgcat_lww jsonb;
BEGIN
	if TG_WHEN = 'AFTER' then
		if (OLD.__pgcat_lww is not null) and
			not (OLD.__pgcat_lww ? v_tombstone_field) then
			v_new_ts = clock_timestamp();
			insert into {{ .Table }} (
					{{range $i,$a := .Row}}
					{{- if gt $i 0 }}, {{end}}{{ $a }}
					{{end -}}
					, __pgcat_lww
				)
				values (
					{{range $i,$a := .Row}}
					{{- if gt $i 0 }}, {{end}}OLD.{{ $a }}
					{{end -}}
					, OLD.__pgcat_lww ||
						('{"' || v_tombstone_field || '": ' ||
						(extract(epoch from v_new_ts)*1000)::bigint ||
						'}')::jsonb ||
						('{"' || v_ts_field || '": "' ||
						v_new_ts::text || '"}')::jsonb
				);
		end if;
		return NULL;
	end if;

	if TG_NARGS > 0 then
		v_is_replica = TG_ARGV[0]::boolean;
	end if;

	select system_identifier from pg_control_system() into v_sysid;

	case TG_OP
		when 'INSERT' then
			if (not v_is_replica) and NEW.__pgcat_lww ? v_tombstone_field then
				return NEW;
			end if;

			select __pgcat_lww into v_pgcat_lww from {{ .Table }} where
				{{range $i,$a := .Identity -}}
				{{if gt $i 0}}and {{end}}{{ $a }} = NEW.{{ $a }}
				{{end -}}
				for update;
			if found then
				if (not v_is_replica) and (v_pgcat_lww ? v_tombstone_field) then
					delete from {{ .Table }} where
						{{range $i,$a := .Identity -}}
						{{if gt $i 0 }}and {{end}}{{ $a }} = NEW.{{ $a }}
						{{end}};
				else
					if v_is_replica then
						update {{ .Table }} set
							{{range .NonIdentCols}}
							{{- . }} = NEW.{{ . }},
							{{end -}}
							__pgcat_lww = NEW.__pgcat_lww
							where
							{{range $i,$a := .Identity -}}
							{{if gt $i 0 }}and {{end}}{{ $a }} = NEW.{{ $a }}
							{{end}};
						return NULL;
					else
						return NEW;
					end if;
				end if;
			end if;

			if not v_is_replica then
				v_new_ts = clock_timestamp();
				NEW.__pgcat_lww = ('{"' || v_ts_field || '": "' ||
					v_new_ts::text || '"}')::jsonb;

				-- individual columns

				{{- range .IndividualCols}}
				NEW.__pgcat_lww = NEW.__pgcat_lww || ('{"ts:{{ . }}": "' ||
					v_new_ts::text || '"}')::jsonb;
				{{- end}}

				-- counter columns

				{{- range .CounterCols}}
				NEW.__pgcat_lww = NEW.__pgcat_lww || ('{"' || 'val:{{ . }}@' ||
					v_sysid || '": ' || NEW.{{ . }} || '}')::jsonb;
				{{- end}}
			end if;

			-- retain our systemid
			NEW.__pgcat_lww = NEW.__pgcat_lww ||
				('{"sysid": ' || v_sysid || '}')::jsonb;
			return NEW;
		when 'UPDATE' then
			if {{range $i,$a := .Identity -}}
				{{if gt $i 0 }}or {{end -}}
				OLD.{{ $a }} != NEW.{{ $a }}
			{{end}}then
				raise 'cannot update the replica ident';
			end if;

			if OLD.__pgcat_lww is null and NEW.__pgcat_lww is not null then
				return NEW;
			end if;

			if v_is_replica then
				v_new_ts = (NEW.__pgcat_lww->>v_ts_field)::timestamp;
			else
				v_new_ts = clock_timestamp();
			end if;

			v_should_update = false;

			if v_new_ts > (OLD.__pgcat_lww->>v_ts_field)::timestamp then
				v_should_update = true;
				NEW.__pgcat_lww = NEW.__pgcat_lww ||
					('{"' || v_ts_field || '": "' ||
					v_new_ts::text || '"}')::jsonb;
			else
				-- set inline columns to old values

				{{- range $i,$a := .InlineCols}}
				NEW.{{ $a }} = OLD.{{ $a }};
				{{- end}}

				-- retain old row timestamp
				NEW.__pgcat_lww = NEW.__pgcat_lww ||
					('{"' || v_ts_field || '": "' ||
					(OLD.__pgcat_lww->>v_ts_field)::text || '"}')::jsonb;
			end if;

			-- individual columns

			{{- range .IndividualCols}}
			-- {{ . }}
			if not NEW.__pgcat_lww ? 'ts:{{ . }}' then
				NEW.__pgcat_lww = NEW.__pgcat_lww ||
					('{"ts:{{ . }}": "' ||
					(NEW.__pgcat_lww->>v_ts_field)::text ||
					'"}')::jsonb;
			end if;

			if not OLD.__pgcat_lww ? 'ts:{{ . }}' then
				OLD.__pgcat_lww = OLD.__pgcat_lww ||
					('{"ts:{{ . }}": "' ||
					(OLD.__pgcat_lww->>v_ts_field)::text ||
					'"}')::jsonb;
			end if;

			if v_is_replica then
				if (NEW.__pgcat_lww->>'ts:{{ . }}')::timestamp <
					(OLD.__pgcat_lww->>'ts:{{ . }}')::timestamp then
					NEW.__pgcat_lww = NEW.__pgcat_lww ||
						('{"ts:{{ . }}": "' ||
						(OLD.__pgcat_lww->>'ts:{{ . }}')::text || '"}')::jsonb;
					NEW.{{ . }} = OLD.{{ . }};
				else
					v_should_update = true;
				end if;
			else
				if NEW.{{ . }} is distinct from OLD.{{ . }} then
					v_should_update = true;
					NEW.__pgcat_lww = NEW.__pgcat_lww ||
						('{"ts:{{ . }}": "' || v_new_ts::text || '"}')::jsonb;
				end if;
			end if;
			{{- end}}

			-- counter columns

			{{- range .CounterCols}}

			-- {{ . }}
			v_name1 = 'val:{{ . }}@' || v_sysid;
			if v_is_replica then
				v_should_update = true;

				v_name2 = 'val:{{ . }}@' || (NEW.__pgcat_lww->>'sysid')::text;

				NEW.__pgcat_lww = NEW.__pgcat_lww ||
					('{"' || v_name2 || '": ' ||
					(NEW.{{ . }})::text || '}')::jsonb;

				-- retain myself
				NEW.__pgcat_lww = NEW.__pgcat_lww ||
					('{"' || v_name1 || '": ' ||
					(OLD.__pgcat_lww->>v_name1)::text || '}')::jsonb;

				-- keep local counter unchanged
				NEW.{{ . }} = OLD.{{ . }};
			else
				if NEW.{{ . }} is distinct from OLD.{{ . }} then
					v_should_update = true;
					NEW.__pgcat_lww = NEW.__pgcat_lww ||
						('{"' || v_name1 || '": ' ||
						NEW.{{ . }} || '}')::jsonb;
				end if;
			end if;
			{{- end}}

			if v_should_update then
				-- retain our systemid
				NEW.__pgcat_lww = NEW.__pgcat_lww ||
					('{"sysid": ' || v_sysid || '}')::jsonb;
				return NEW;
			else
				-- ignore update
				return NULL;
			end if;
		when 'DELETE' then
			-- ignore delete from replica
			if v_is_replica then
				return NULL;
			end if;

			return OLD;
	end case;

	return NULL;
END;
$$ LANGUAGE plpgsql;

do $$
begin
CREATE TRIGGER {{ .TableName }}_pgcat_lww_trigger1
	BEFORE INSERT OR UPDATE OR DELETE ON {{ .Table }}
	FOR EACH ROW EXECUTE FUNCTION {{ .Table }}_pgcat_lww_trigger();
exception when duplicate_object then
	-- do nothing
end
$$;

do $$
begin
CREATE TRIGGER {{ .TableName }}_pgcat_lww_trigger2
	AFTER DELETE ON {{ .Table }}
	FOR EACH ROW EXECUTE FUNCTION {{ .Table }}_pgcat_lww_trigger();
exception when duplicate_object then
	-- do nothing
end
$$;

do $$
begin
CREATE TRIGGER {{ .TableName }}_pgcat_lww_trigger3
	BEFORE INSERT OR UPDATE OR DELETE ON {{ .Table }}
	FOR EACH ROW EXECUTE FUNCTION {{ .Table }}_pgcat_lww_trigger(true);
exception when duplicate_object then
	-- do nothing
end
$$;

alter table {{ .Table }} enable replica trigger
	{{ .TableName }}_pgcat_lww_trigger3;

-- view

create or replace view {{ .Table }}_pgcat_lww_view as
select {{range $i,$a := .Row}}{{if gt $i 0 }} , {{end}}{{ $a }}{{end}}
from {{ .Table }}
where not __pgcat_lww ? '__deleted';

-- counter function

CREATE or replace FUNCTION
pgcat_lww_counter(jsonb, text) RETURNS bigint AS $$
	select sum(value::bigint)::bigint from jsonb_each_text($1)
		where key like 'val:' || $2 || '@%';
$$ LANGUAGE sql;

-- tombstone index

create index IF NOT EXISTS {{ .TableName }}_pgcat_lww_tombstone_idx
	on {{ .Table }} using btree(((__pgcat_lww->>'__deleted')::bigint))
	where __pgcat_lww ? '__deleted';

-- vaccum function

CREATE or replace FUNCTION
{{ .Table }}_pgcat_lww_vaccum(ago interval) RETURNS int AS $$
declare
	v_row_count int;
	v_now bigint = (extract(epoch from (clock_timestamp()-ago))*1000)::bigint;
begin
	delete from {{ .Table }} where __pgcat_lww ? '__deleted' and
		(__pgcat_lww->>'__deleted')::bigint < v_now;
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	return v_row_count;
END;
$$ LANGUAGE plpgsql;
