-- FUNCTION: public.truncate_schema(character varying)

-- DROP FUNCTION public.truncate_schema(character varying);

CREATE OR REPLACE FUNCTION public.truncate_schema(
	schema_name character varying)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
    statements CURSOR FOR
        SELECT schemaname, tablename FROM pg_tables
        WHERE schemaname = schema_name;
BEGIN
    FOR stmt IN statements LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(stmt.schemaname) || '.' || quote_ident(stmt.tablename) || ' CASCADE;';
    END LOOP;
END
$BODY$;

	
-- pre_actions function
-- 

CREATE OR REPLACE FUNCTION public.drop_all_constraints(inputschema varchar) RETURNS void AS $$
DECLARE
  i RECORD;
BEGIN
	  FOR i IN 
		(SELECT 
			FORMAT('ALTER TABLE ONLY %s."%s" DROP CONSTRAINT "%s";', nspname, relname, conname) AS  drop_constraint_command
			FROM pg_constraint
			INNER JOIN pg_class ON conrelid=pg_class.oid
			INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
			WHERE nspname = inputschema
			ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END,contype,nspname,relname,conname)
	  LOOP
		RAISE INFO 'DROPING CONSTRAINT: %', i.drop_constraint_command;
		EXECUTE i.drop_constraint_command;
	  END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.drop_all_triggers(inputschema varchar) RETURNS void AS $$
DECLARE
  i RECORD;
BEGIN
	  FOR i IN 
		(SELECT 
			FORMAT('DROP TRIGGER IF EXISTS "%s" ON %s."%s";', trigger_name, trigger_schema, event_object_table) AS drop_trigger_command
			FROM  information_schema.triggers
			WHERE trigger_schema = inputschema -- Your schema name comes here
			ORDER BY event_object_table
			,event_manipulation)
	  LOOP
		RAISE INFO 'DROPING TRIGGER: %', i.drop_trigger_command;
		EXECUTE i.drop_trigger_command;
	  END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.drop_all_indexes(inputschema varchar) RETURNS void AS $$
DECLARE
  i RECORD;
BEGIN
	  FOR i IN 
		(SELECT FORMAT('DROP INDEX IF EXISTS %s."%s";', schemaname, indexname) AS drop_index_command
			FROM pg_indexes
			WHERE schemaname = inputschema)
	  LOOP
		RAISE INFO 'DROPING INDEX: %', i.drop_index_command;
		EXECUTE i.drop_index_command;
	  END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.refreshAllMaterializedViews(schema_arg TEXT DEFAULT 'public')
RETURNS INT AS $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE 'Refreshing materialized view in schema %', schema_arg;
    FOR r IN SELECT matviewname FROM pg_matviews WHERE schemaname = schema_arg 
    LOOP
        RAISE NOTICE 'Refreshing materialized view %.%', schema_arg, r.matviewname;
        EXECUTE 'REFRESH MATERIALIZED VIEW ' || schema_arg || '.' || r.matviewname; 
    END LOOP;

    RETURN 1;
END 
$$ LANGUAGE plpgsql;

-- SELECT row_count_all_tables('oss');
CREATE OR REPLACE FUNCTION public.row_count_all_tables(schema_name text default 'public')
  RETURNS table(table_name text, cnt bigint) as
$$
declare
 table_name text;
begin
  for table_name in SELECT c.relname FROM pg_class c
    JOIN pg_namespace s ON (c.relnamespace=s.oid)
    WHERE c.relkind = 'r' AND s.nspname=schema_name ORDER BY c.relname
  LOOP
    RETURN QUERY EXECUTE format('select cast(%L as text),count(*) from %I.%I',
       table_name, schema_name, table_name);
  END LOOP;
end
$$ language plpgsql;


-- SELECT row_count_all_views('pba');
CREATE OR REPLACE FUNCTION public.row_count_all_views(schema_name text default 'public')
  RETURNS table(view_name text, cnt bigint) as
$$
declare
 view_name text;
BEGIN
  FOR view_name IN SELECT viewname from pg_catalog.pg_views
	WHERE schemaname = schema_name
	ORDER BY viewname
  LOOP
    RETURN QUERY EXECUTE format('select cast(%L as text),count(*) from %I.%I',
       view_name, schema_name, view_name);
  END LOOP;
END
$$ LANGUAGE plpgsql;
		
-- SELECT row_count_all_materialized_views('public');		
CREATE OR REPLACE FUNCTION public.row_count_all_materialized_views(schema_name text default 'public')
  RETURNS table(mview_name text, cnt bigint) as
$$
declare
 mview_name text;
BEGIN
  FOR mview_name IN SELECT matviewname      
	FROM pg_matviews WHERE schemaname = schema_name
	ORDER BY matviewname
  LOOP
    RETURN QUERY EXECUTE format('select cast(%L as text),count(*) from %I.%I',
       mview_name, schema_name, mview_name);
  END LOOP;
END
$$ LANGUAGE plpgsql;
		

