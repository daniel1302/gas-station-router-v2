#!/bin/sh
set -eu
umask 077

validate_environment() {
  : "${GAS_STATION_PRIVATE_KEY:?Missing sponsor key}"
  : "${IOTA_GRPC_URL:?Missing Foundation gRPC node URL}"
  case "$GAS_STATION_PRIVATE_KEY" in
    *[!A-Za-z0-9+/=]* | '')
      echo 'Expected a base64 sponsor key' >&2
      exit 1
      ;;
  esac
  case "$REDIS_DB" in
    1 | 2 | 3) ;;
    *)
      echo 'Expected REDIS_DB 1, 2 or 3' >&2
      exit 1
      ;;
  esac
  case "$IOTA_GRPC_URL" in
    *[!A-Za-z0-9:/._-]* | '')
      echo 'Expected a plain http(s) fullnode URL' >&2
      exit 1
      ;;
  esac
}

render_config() {
  awk '{
    gsub(/\$\{GAS_STATION_PRIVATE_KEY\}/, ENVIRON["GAS_STATION_PRIVATE_KEY"])
    gsub(/\$\{REDIS_DB\}/, ENVIRON["REDIS_DB"])
    gsub(/\$\{IOTA_GRPC_URL\}/, ENVIRON["IOTA_GRPC_URL"])
    print
  }' /config/gas-station.config.yaml >/run/gas-station/config.yaml
}

validate_environment
render_config
unset GAS_STATION_PRIVATE_KEY
exec /usr/local/bin/entrypoint --config-path /run/gas-station/config.yaml
