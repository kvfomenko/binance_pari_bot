--under bina
CREATE SCHEMA IF NOT EXISTS bina;
GRANT ALL ON SCHEMA bina TO bina;

GRANT USAGE ON SCHEMA bina TO bina_i;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bina TO bina_i;


--under bina
CREATE SCHEMA IF NOT EXISTS util;
GRANT USAGE ON SCHEMA util TO bina;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA util TO bina;



create extension if not exists dblink;

GRANT EXECUTE ON FUNCTION dblink_connect_u(TEXT) TO bina;
grant EXECUTE on FUNCTION dblink_connect_u(TEXT, TEXT) to bina;


