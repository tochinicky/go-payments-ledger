-- Login roles for the services, run after the migrations (which create the group roles that hold the grants).
-- Dev-only passwords: a real deployment creates these from its secret store.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ledger_app') THEN
        CREATE ROLE ledger_app LOGIN PASSWORD 'ledger_app' IN ROLE ledger_writer;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'notifier_app') THEN
        CREATE ROLE notifier_app LOGIN PASSWORD 'notifier_app' IN ROLE notifier_writer;
    END IF;
END
$$;
