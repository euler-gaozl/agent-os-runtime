# Runtime ownership

Read README.md. This repository owns the execution transaction domain. Worker consumes only the versioned execution package, which must expose no database, transaction, raw SQL, migration or local tool invocation. Keep model/tool intent, evidence persistence and receipts ordered under the original fence. Notes and local tool receipts stay atomic here. No foreign service SQL fallback. Do not copy Registry/Storage/Knowledge/Sandbox implementations. Preserve historical engines and replay behavior. Use Go and bounded resources; no secrets/user data in Git.
