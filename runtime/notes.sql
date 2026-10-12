-- Runtime-only lifecycle index. Knowledge imports have their own lifecycle.
CREATE INDEX IF NOT EXISTS documents_supersedes ON documents(supersedes);
CREATE TABLE IF NOT EXISTS note_roots(
 root_id text PRIMARY KEY REFERENCES documents(id),
 current_id text REFERENCES documents(id),
 state text NOT NULL CHECK(state IN ('active','retired','conflict')),
 retired_at timestamptz, reason text NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS note_versions(
 document_id text PRIMARY KEY REFERENCES documents(id),
 root_id text NOT NULL REFERENCES note_roots(root_id));
CREATE INDEX IF NOT EXISTS note_versions_root ON note_versions(root_id);
CREATE TABLE IF NOT EXISTS note_admin_receipts(
 request_id text PRIMARY KEY, request_hash text NOT NULL, response jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS note_audits(
 seq bigserial PRIMARY KEY, root_id text NOT NULL REFERENCES note_roots(root_id),
 document_id text NOT NULL REFERENCES documents(id), action text NOT NULL,
 reason text NOT NULL, actor_kind text NOT NULL, actor_id text NOT NULL,
 run_id text REFERENCES runs(id), operation_id text UNIQUE REFERENCES calls(id) DEFERRABLE INITIALLY DEFERRED,
 request_id text UNIQUE REFERENCES note_admin_receipts(request_id) DEFERRABLE INITIALLY DEFERRED,
 created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS note_audits_root ON note_audits(root_id,seq);

-- Preserve every historical branch. Multiple leaves mean conflict, never
-- 'newest wins'. Existing roots (including withdrawn ones) are not reset.
WITH RECURSIVE tree AS (
 SELECT d.id,d.id AS root_id FROM documents d WHERE supersedes IS NULL
 AND NOT EXISTS(SELECT 1 FROM note_roots r WHERE r.root_id=d.id)
 UNION ALL SELECT d.id,t.root_id FROM documents d JOIN tree t ON d.supersedes=t.id
), leaves AS (
 SELECT t.* FROM tree t WHERE NOT EXISTS(SELECT 1 FROM documents d WHERE d.supersedes=t.id)
) INSERT INTO note_roots(root_id,current_id,state)
SELECT root_id,CASE WHEN count(*)=1 THEN min(id) END,
 CASE WHEN count(*)=1 THEN 'active' ELSE 'conflict' END FROM leaves GROUP BY root_id;
WITH RECURSIVE tree AS (
 SELECT d.id,d.id AS root_id FROM documents d WHERE supersedes IS NULL
 AND NOT EXISTS(SELECT 1 FROM note_versions v WHERE v.document_id=d.id)
 UNION ALL SELECT d.id,t.root_id FROM documents d JOIN tree t ON d.supersedes=t.id
) INSERT INTO note_versions(document_id,root_id) SELECT id,root_id FROM tree;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM documents d LEFT JOIN note_versions v ON v.document_id=d.id WHERE v.document_id IS NULL)
 THEN RAISE EXCEPTION 'note lineage migration requires investigation; no document has been changed'; END IF;
END $$;

-- Protect every INSERT path, including old management POST /documents. The
-- parent row alone is not the serialization point: all versions share a root.
CREATE OR REPLACE FUNCTION register_note_version() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE root text; head text; state_now text; previous_kind text; previous_scope text;
 actor jsonb := COALESCE(NULLIF(current_setting('agentos.note_mutation',true),''),'{}')::jsonb;
BEGIN
 IF NEW.supersedes IS NULL THEN
  root := NEW.id;
  INSERT INTO note_roots(root_id,current_id,state) VALUES(root,NEW.id,'active');
 ELSE
  SELECT v.root_id,d.kind,d.scope INTO STRICT root,previous_kind,previous_scope
   FROM note_versions v JOIN documents d ON d.id=v.document_id WHERE d.id=NEW.supersedes;
  PERFORM 1 FROM note_roots WHERE root_id=root FOR UPDATE;
  -- Separate statement after waiting for the lock: use a fresh READ COMMITTED snapshot.
  SELECT current_id,state INTO head,state_now FROM note_roots WHERE root_id=root;
  IF state_now<>'active' OR head IS DISTINCT FROM NEW.supersedes THEN
   RAISE EXCEPTION 'note is no longer the active head, is retired, or has unresolved branches' USING ERRCODE='P4090';
  END IF;
  IF previous_kind<>NEW.kind OR previous_scope<>NEW.scope THEN
   RAISE EXCEPTION 'revisions must preserve kind and scope';
  END IF;
  IF previous_scope LIKE 'knowledge:%' THEN
   RAISE EXCEPTION 'historical Knowledge copies are read-only; manage the source in Knowledge';
  END IF;
  UPDATE note_roots SET current_id=NEW.id WHERE root_id=root;
 END IF;
 INSERT INTO note_versions(document_id,root_id) VALUES(NEW.id,root);
 INSERT INTO note_audits(root_id,document_id,action,reason,actor_kind,actor_id,run_id,operation_id,request_id)
 VALUES(root,NEW.id,CASE WHEN NEW.supersedes IS NULL THEN 'create' ELSE 'revise' END,
 COALESCE(actor->>'reason','legacy write without supplied reason'),
 COALESCE(actor->>'kind','legacy'),COALESCE(actor->>'id','unattributed'),
 actor->>'run_id',actor->>'operation_id',actor->>'request_id');
 RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER note_version_insert AFTER INSERT ON documents
 FOR EACH ROW EXECUTE FUNCTION register_note_version();
