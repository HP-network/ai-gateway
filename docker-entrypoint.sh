#!/bin/sh
set -eu

key_file=/data/generated-keys.env
mkdir -p /data

provided_app_key=${AI_GATEWAY_API_KEY:-}
provided_admin_key=${AI_GATEWAY_ADMIN_API_KEY:-}
if [ -s "$key_file" ]; then
  # shellcheck disable=SC1090
  . "$key_file"
fi
[ -n "$provided_app_key" ] && AI_GATEWAY_API_KEY=$provided_app_key
[ -n "$provided_admin_key" ] && AI_GATEWAY_ADMIN_API_KEY=$provided_admin_key
if [ -z "${AI_GATEWAY_API_KEY:-}" ] || [ -z "${AI_GATEWAY_ADMIN_API_KEY:-}" ]; then
  umask 077
  if [ -z "${AI_GATEWAY_API_KEY:-}" ]; then
    AI_GATEWAY_API_KEY="ag_$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  fi
  if [ -z "${AI_GATEWAY_ADMIN_API_KEY:-}" ]; then
    AI_GATEWAY_ADMIN_API_KEY="adm_$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  fi
fi
umask 077
printf 'AI_GATEWAY_API_KEY=%s\nAI_GATEWAY_ADMIN_API_KEY=%s\n' "$AI_GATEWAY_API_KEY" "$AI_GATEWAY_ADMIN_API_KEY" > "$key_file"
export AI_GATEWAY_API_KEY AI_GATEWAY_ADMIN_API_KEY

if [ "${AI_GATEWAY_PRINT_KEYS:-1}" = "1" ]; then
  printf 'AI Gateway keys: read them with `docker compose exec ai-gateway cat /data/generated-keys.env`\n'
fi

exec /usr/local/bin/ai-gateway "$@"
