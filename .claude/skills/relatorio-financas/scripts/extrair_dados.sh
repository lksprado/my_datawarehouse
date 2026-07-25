#!/usr/bin/env bash
# Extrai o pacote JSON do domínio finanças para um mês de referência.
#
#   extrair_dados.sh [MES_REF] [ARQUIVO_SAIDA]
#
#   MES_REF        AAAA-MM ou AAAA-MM-DD. Default: mês anterior ao atual.
#   ARQUIVO_SAIDA  Default: stdout.
#
# A conexão sai do profiles.yml do dbt (target `dev` do perfil
# `my_datawarehouse`), para não duplicar credenciais.
set -euo pipefail

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJETO="$(cd "$AQUI/../../../.." && pwd)"
PROFILES="${DBT_PROFILES_DIR:-$HOME/.dbt}/profiles.yml"

MES_REF="${1:-$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-01)}"
SAIDA="${2:-}"

# Aceita AAAA-MM e normaliza para o primeiro dia do mês.
[[ "$MES_REF" =~ ^[0-9]{4}-[0-9]{2}$ ]] && MES_REF="${MES_REF}-01"
if ! [[ "$MES_REF" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "erro: MES_REF inválido: '$MES_REF' (use AAAA-MM ou AAAA-MM-DD)" >&2
    exit 2
fi
MES_REF="$(date -d "$MES_REF" +%Y-%m-01)"

[[ -f "$PROFILES" ]] || { echo "erro: profiles.yml não encontrado em $PROFILES" >&2; exit 2; }

eval "$(python3 - "$PROFILES" <<'PY'
import sys, yaml, shlex
perfis = yaml.safe_load(open(sys.argv[1]))
alvo = perfis["my_datawarehouse"]["outputs"]["dev"]
for var, chave in (("PGHOST","host"), ("PGPORT","port"), ("PGUSER","user"),
                   ("PGPASSWORD","pass"), ("PGDATABASE","dbname")):
    print(f"export {var}={shlex.quote(str(alvo[chave]))}")
PY
)"

executar() {
    psql -q -X -At -v ON_ERROR_STOP=1 -v mes_ref="$MES_REF" \
        -f "$AQUI/../queries/extrair.sql"
}

if [[ -n "$SAIDA" ]]; then
    mkdir -p "$(dirname "$SAIDA")"
    executar > "$SAIDA"
    echo "dados extraídos: $SAIDA (mes_ref=$MES_REF)" >&2
else
    executar
fi
