#!/usr/bin/env bash
# Geração em lote dos relatórios financeiros mensais.
#
# NÃO está plugado em nenhum agendador. O caminho normal é interativo — a skill
# `/relatorio-financas` no terminal, que resolve o mês sozinha. Este script é
# para o caso em lote: refazer meses antigos sem sessão interativa.
#
#   gerar_relatorios_financas.sh [AAAA-MM] [--forcar]
#
# Sem argumento de mês: mês anterior ao atual.
# --forcar: gera mesmo que o mês não tenha fechado (o PDF sai com tarja).
#
# Variáveis de ambiente:
#   RELATORIOS_DIR   destino dos PDFs (default: <projeto>/relatorios/AAAA-MM)
#   DBT_PROFILES_DIR diretório do profiles.yml (default: ~/.dbt)
#   CLAUDE_BIN       caminho do CLI (default: `claude` no PATH)
#   TIMEOUT_SEG      teto de execução (default: 900)
#   PERMITIR_INCOMPLETO=1  mesmo efeito de --forcar
#
# Saída: 0 com os dois PDFs gerados
#        75 mês ainda não fechou (não é erro — tente de novo depois)
#        outros != 0 para falha de verdade
#
# Backfill:
#   for m in 2026-04 2026-05 2026-06; do
#       scripts/gerar_relatorios_financas.sh "$m" || echo "pulou $m"
#   done
set -euo pipefail

PROJETO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
TIMEOUT_SEG="${TIMEOUT_SEG:-900}"
FORCAR="${PERMITIR_INCOMPLETO:-0}"
MES_REF=""

for arg in "$@"; do
    case "$arg" in
        --forcar) FORCAR=1 ;;
        -*)       echo "erro: opção desconhecida '$arg'" >&2; exit 2 ;;
        *)        MES_REF="$arg" ;;
    esac
done

MES_REF="${MES_REF:-$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m)}"
if ! [[ "$MES_REF" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "erro: mês inválido '$MES_REF' (esperado AAAA-MM)" >&2
    exit 2
fi

DESTINO="${RELATORIOS_DIR:-$PROJETO/relatorios/$MES_REF}"
mkdir -p "$DESTINO"

command -v "$CLAUDE_BIN" >/dev/null 2>&1 || {
    echo "erro: CLI '$CLAUDE_BIN' não encontrado no PATH" >&2; exit 3; }

echo "[relatorios] projeto=$PROJETO mes=$MES_REF destino=$DESTINO"

# Extrai antes de chamar o agente: falha cedo se o banco não responder, e é
# daqui que sai o portão de prontidão.
DADOS="$DESTINO/.dados_$MES_REF.json"
"$PROJETO/.claude/skills/relatorio-financas/scripts/extrair_dados.sh" \
    "$MES_REF" "$DADOS"

# Portão: mês que ainda não terminou de carregar produz relatório errado com
# cara de certo. Sair com 75 em vez de 1 para o chamador distinguir "ainda não"
# de "quebrou".
PRONTO="$(python3 - "$DADOS" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))["meta"]["prontidao"]
print("1" if p["pronto"] else "0")
for frase in p.get("pendencias") or []:
    print("  - " + frase, file=sys.stderr)
if not p["pronto"] and p.get("ultimo_mes_fechado"):
    print("  último mês fechado: " + p["ultimo_mes_fechado"][:7], file=sys.stderr)
PY
)"

if [[ "$PRONTO" != "1" ]]; then
    if [[ "$FORCAR" != "1" ]]; then
        echo "[relatorios] $MES_REF ainda não fechou — nada gerado (use --forcar para ignorar)" >&2
        rm -f "$DADOS"
        exit 75
    fi
    echo "[relatorios] AVISO: $MES_REF não fechou; gerando mesmo assim (--forcar)" >&2
fi

cd "$PROJETO"
set +e
RELATORIOS_DIR="$DESTINO" timeout "$TIMEOUT_SEG" "$CLAUDE_BIN" \
    --print \
    --permission-mode acceptEdits \
    --allowedTools "Bash,Read,Write,Edit,Glob,Grep,Skill" \
    "/relatorio-financas mês de referência $MES_REF-01. Grave os PDFs em $DESTINO. Modo não interativo: não faça perguntas, use os defaults do glossário para qualquer parâmetro marcado [CONFIRMAR] e registre no rodapé quais premissas foram usadas.$([[ "$PRONTO" != "1" ]] && echo ' O portão de prontidão reprovou este mês e a geração foi forçada: diga no diagnóstico que os valores estão parciais.')"
STATUS=$?
set -e

if [[ $STATUS -ne 0 ]]; then
    echo "erro: geração falhou (status $STATUS)" >&2
    exit $STATUS
fi

# O agente pode errar o nome; valida por contagem e não por caminho exato.
mapfile -t PDFS < <(find "$DESTINO" -maxdepth 1 -name '*.pdf' -newermt '-1 hour' | sort)
if [[ ${#PDFS[@]} -lt 2 ]]; then
    echo "erro: esperava 2 PDFs em $DESTINO, encontrei ${#PDFS[@]}" >&2
    printf '  %s\n' "${PDFS[@]:-（nenhum）}" >&2
    exit 4
fi

for p in "${PDFS[@]}"; do
    tam=$(stat -c%s "$p")
    if [[ $tam -lt 20000 ]]; then
        echo "erro: $p tem apenas ${tam}B — provavelmente saiu vazio" >&2
        exit 5
    fi
    echo "[relatorios] ok: $p ($(du -h "$p" | cut -f1))"
done

rm -f "$DADOS"
echo "[relatorios] concluído para $MES_REF"
