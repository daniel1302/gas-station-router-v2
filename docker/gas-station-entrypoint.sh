#!/bin/sh
set -eu
umask 077

: "${GAS_STATION_PRIVATE_KEY:?Missing sponsor key}"
: "${IOTA_GRPC_URL:?Missing Foundation gRPC node URL}"
case "$GAS_STATION_PRIVATE_KEY" in
  *[!A-Za-z0-9+/=]*|'') echo 'Expected a base64 sponsor key' >&2; exit 1 ;;
esac
case "$REDIS_DB" in
  1|2|3) ;;
  *) echo 'Expected REDIS_DB 1, 2 or 3' >&2; exit 1 ;;
esac
case "$IOTA_GRPC_URL" in
  *[!A-Za-z0-9:/._-]*|'') echo 'Expected a plain http(s) fullnode URL' >&2; exit 1 ;;
esac

sed -e "s|\${GAS_STATION_PRIVATE_KEY}|$GAS_STATION_PRIVATE_KEY|g" \
    -e "s|\${REDIS_DB}|$REDIS_DB|g" \
    -e "s|\${IOTA_GRPC_URL}|$IOTA_GRPC_URL|g" \
    /config/gas-station.config.yaml > /run/gas-station/config.yaml
unset GAS_STATION_PRIVATE_KEY
exec /usr/local/bin/entrypoint --config-path /run/gas-station/config.yaml
