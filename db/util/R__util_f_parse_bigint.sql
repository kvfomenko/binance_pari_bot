CREATE OR REPLACE FUNCTION util.f_parse_bigint(av_number VARCHAR)
    RETURNS BIGINT
    IMMUTABLE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ln_result BIGINT;
BEGIN
    IF av_number IS NULL OR av_number = '' THEN
        RETURN NULL;
    ELSE
        BEGIN
            ln_result := av_number::BIGINT;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN NULL;
        END;
    END IF;

    RETURN ln_result;
END;
$function$
