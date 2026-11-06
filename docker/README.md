# E2E — bardzo prosto

Wymagania: Docker Compose, Rust 1.95 i repo `gas-station` obok tego repo
(na tej maszynie już jest). Pierwsza kompilacja potrwa kilka minut.

## 1. Przygotuj klucze i doładuj sponsorów

W katalogu repozytorium:

```sh
cd docker
./e2e.sh init       # tylko gdy nie masz jeszcze .env
./e2e.sh sponsors   # pokaż adresy do doładowania
```

Doładuj **wszystkie 3 adresy**, np. po 10 testowych IOTA, przez
<https://faucet.testnet.iota.cafe/>. Nadawców transakcji nie trzeba doładowywać.
Klucze są w `.env` (uprawnienia `600`, ignorowany przez Git).

## 2. Uruchom stacje i router

Nadal w `docker/`:

```sh
docker compose up -d --build
./start-router.sh
```

Zostaw terminal otwarty. Stacje mają porty `9527–9529`, router `9257`.
Po doładowaniu poczekaj na inicjalizację monet: `docker compose logs -f`.

## 3. Uruchom test w drugim terminalu

W katalogu `docker/`:

```sh
./e2e.sh reserve
```

`PASS` = 9 rezerwacji, poprawne monety, unikalne ID i co najmniej 3 sponsorów.
Budżet jest losowy, rezerwacje wygasają po 5 sekundach. Błąd daje niezerowy kod wyjścia.

Opcjonalnie:

```sh
ROUTER_URL=http://127.0.0.1:9257 ./e2e.sh reserve --count 20
./e2e.sh reserve --count 3 --min-sponsors 1   # test jednego backendu
./e2e.sh transactions --count 9             # pełne transakcje, na później
```

**Obecnie `transactions` zgłasza `501 not implemented` przed rezerwacją**,
bo router nie wykonuje jeszcze transakcji. Po dodaniu wykonania będzie wysyłać
podpisane transakcje z losowym nadawcą i budżetem, odczytujące zegar IOTA 1–5 razy,
przez `/v1/execute_transaction`, i sprawdzać ich wynik niezależnie na fullnode.

Domyślny `ROUTER_URL`: `http://127.0.0.1:9257`. `.env` dostarcza `GS_AUTH`,
`IOTA_RPC_URL=https://api.testnet.iota.cafe` i
`IOTA_GRPC_URL=https://grpc.testnet.iota.cafe` — Foundation Testnet.
Zmienne z terminala mają pierwszeństwo. Inną ścieżkę do źródeł stacji ustawisz
przez `GAS_STATION_SOURCE` w `.env` (domyślnie `../../gas-station`, względem `docker/`).

Redis przechowuje dane stacji w osobnych bazach `1–3`; router używa RAM
i traci mapowania rezerwacji po restarcie. Nie zmieniaj kluczy przy istniejącym Redis.
Zatrzymanie: `Ctrl+C` dla routera, `docker compose down` dla stacji (zachowuje dane).
