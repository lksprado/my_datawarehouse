#!/usr/bin/env bash
# Extrai o pacote JSON do relatório de meio de mês.
#
#   extrair_dados.sh [HOJE] [ARQUIVO_SAIDA]
#
#   HOJE           AAAA-MM-DD. Default: a data de hoje.
#   ARQUIVO_SAIDA  Default: stdout.
#
# Diferente do relatório de fechamento, o parâmetro é uma DATA e não um mês: a
# pergunta é "como está o mês a esta altura". Travar HOJE num dia já passado
# reproduz o relatório daquele dia — é assim que se afere se a projeção acertou.
#
# A conexão sai do profiles.yml do dbt (target `dev` do perfil
# `my_datawarehouse`), para não duplicar credenciais.
set -euo pipefail

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROFILES="${DBT_PROFILES_DIR:-$HOME/.dbt}/profiles.yml"

HOJE="${1:-$(date +%F)}"
SAIDA="${2:-}"

if ! [[ "$HOJE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "erro: HOJE inválido: '$HOJE' (use AAAA-MM-DD)" >&2
    exit 2
fi
HOJE="$(date -d "$HOJE" +%F)"

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
    psql -q -X -At -v ON_ERROR_STOP=1 -v hoje="$HOJE" \
        -f "$AQUI/../queries/extrair_meio_mes.sql"
}

if [[ -n "$SAIDA" ]]; then
    mkdir -p "$(dirname "$SAIDA")"
    executar > "$SAIDA"
    echo "dados extraídos: $SAIDA (hoje=$HOJE)" >&2
else
    executar
fi
