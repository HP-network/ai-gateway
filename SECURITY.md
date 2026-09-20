# Security

Do not open a public issue for a credential leak or a reproducible security problem involving a live provider. Email `hpnetwork@hpnetwork.top` with the affected version, reproduction steps, and a sanitized example.

The gateway does not persist prompts or provider responses. SQLite stores only hashed managed API keys and aggregate request/token counters. Deployments should protect both gateway and admin bearer tokens, mount the database with restrictive permissions, restrict network access, and avoid logging request bodies at the reverse proxy.

The plaintext value returned by `POST /admin/api-keys` cannot be recovered later. Treat it like a password and rotate it by revoking the key and creating a replacement.
