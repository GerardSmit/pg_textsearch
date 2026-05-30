-- Upgrade from 2.0.0 to 2.1.0
-- No SQL catalog changes.

-- Verify loaded library matches this SQL script version.
DO $$
DECLARE
    lib_ver text;
    lib_parts int[];
BEGIN
    lib_ver := pg_catalog.current_setting('pg_textsearch.library_version', true);
    IF lib_ver IS NULL THEN
        RAISE EXCEPTION
            'pg_textsearch library not loaded. '
            'Add pg_textsearch to shared_preload_libraries and restart.';
    END IF;

    lib_parts := pg_catalog.string_to_array(
        pg_catalog.regexp_replace(lib_ver, '-.*$', ''),
        '.')::int[];
    IF lib_parts OPERATOR(pg_catalog.<) ARRAY[2, 1, 0] THEN
        RAISE EXCEPTION
            'pg_textsearch library version mismatch: loaded=%, expected >= %. '
            'Restart the server after installing the new binary.',
            lib_ver, '2.1.0';
    END IF;
END $$;

DO $$
BEGIN
    RAISE INFO 'pg_textsearch upgraded to v2.1.0';
END
$$;
