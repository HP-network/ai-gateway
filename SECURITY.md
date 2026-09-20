# Security

Do not open a public issue for a credential leak or a reproducible security problem involving a live provider. Email `hpnetwork@hpnetwork.top` with the affected version, reproduction steps, and a sanitized example.

The gateway does not persist prompts or provider responses. Deployments should still protect the gateway bearer token, restrict network access, and avoid logging request bodies at the reverse proxy.
