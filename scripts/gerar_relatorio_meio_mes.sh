#!/usr/bin/env bash
# Geração em lote do relatório de meio de mês.
#
# NÃO está plugado em nenhum agendador. O caminho normal é interativo — a skill
# `/relatorio-meio-mes` no terminal, que resolve a data sozinha. Este script é
# para o caso em lote: reproduzir o relatório de uma data já passada sem sessão
# interativa, que é também como se afere se a projeção acertou.
#
#   gerar_relatorio_meio_mes.sh [AAAA-MM-DD] [--forcar]
#
# Sem argumento: hoje.
# --forcar: gera mesmo com o portão de ritmo reprovado (o PDF sai com tarja).
#
# Variáveis de ambiente:
#   RELATORIOS_DIR   destino do PDF (default: <projeto>/relatorios/AAAA-MM)
#   DBT_PROFILES_DIR diretório do profiles.yml (default: ~/.dbt)
#   CLAUDE_BIN       caminho do CLI (default: `claude` no PATH)
#   TIMEOUT_SEG      teto de execução (default: 900)
#   PERMITIR_INCOMPLETO=1  mesmo efeito de --forcar
#
# Os dois portões são independentes: `pronto_ritmo` decide se há relatório, e
# `pronto_indicadores` decide apenas se a seção de desempenho entra. Indexador
# ainda não publicado não é motivo para não gerar.
#
# Saída: 0 PDF gerado
#        75 o mês ainda não sustenta ritmo (não é erro — tente de novo depois)
#        outros != 0 para falha de verdade
#
# Aferição da projeção (o teste que decide se o relatório vale):
#   for d in 2026-05-17 2026-06-17 2026-07-17; do
#       scripts/gerar_relatorio_meio_mes.sh "$d" || echo "pulou $d"
#   done
#   # depois compare projecao.total com marts.resultado.total_despesas do mês
set -euo pipefail

PROJETO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
TIMEOUT_SEG="${TIMEOUT_SEG:-900}"
FORCAR="${PERMITIR_INCOMPLETO:-0}"
HOJE=""

for arg in "$@"; do
    case "$arg" in
        --forcar) FORCAR=1 ;;
        -*)       echo "erro: opção desconhecida '$arg'" >&2; exit 2 ;;
        *)        HOJE="$arg" ;;
    esac
done

HOJE="${HOJE:-$(date +%F)}"
if ! [[ "$HOJE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "erro: data inválida '$HOJE' (esperado AAAA-MM-DD)" >&2
    exit 2
fi
MES="${HOJE:0:7}"

DESTINO="${RELATORIOS_DIR:-$PROJETO/relatorios/$MES}"
mkdir -p "$DESTINO"

command -v "$CLAUDE_BIN" >/dev/null 2>&1 || {
    echo "erro: CLI '$CLAUDE_BIN' não encontrado no PATH" >&2; exit 3; }

echo "[meio-mes] projeto=$PROJETO hoje=$HOJE destino=$DESTINO"

# Extrai ANTES de chamar o agente: falha cedo se o banco não responde, e o
# portão é lido aqui em vez de depender de o agente obedecer.
DADOS="$DESTINO/.dados_meio_mes_$HOJE.json"
"$PROJETO/.claude/skills/relatorio-meio-mes/scripts/extrair_dados.sh" "$HOJE" "$DADOS"

leitura="$(python3 - "$DADOS" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))["meta"]["prontidao"]
print(int(bool(p.get("pronto_ritmo"))), int(bool(p.get("pronto_indicadores"))))
print("|".join(p.get("pendencias_ritmo") or []))
PY
)"
PRONTO_RITMO="$(echo "$leitura" | head -1 | cut -d' ' -f1)"
PRONTO_IND="$(echo "$leitura" | head -1 | cut -d' ' -f2)"
PENDENCIAS="$(echo "$leitura" | tail -1)"

echo "[meio-mes] pronto_ritmo=$PRONTO_RITMO pronto_indicadores=$PRONTO_IND"

if [[ "$PRONTO_RITMO" != "1" && "$FORCAR" != "1" ]]; then
    echo "[meio-mes] o mês ainda não sustenta leitura de ritmo:" >&2
    echo "$PENDENCIAS" | tr '|' '\n' | sed 's/^/    - /' >&2
    rm -f "$DADOS"
    exit 75
fi

EXTRA=""
[[ "$PRONTO_IND" != "1" ]] && EXTRA=" Os indexadores do mês anterior não estão publicados: omita a seção de desempenho."
[[ "$PRONTO_RITMO" != "1" ]] && EXTRA="$EXTRA Gere mesmo com o portão de ritmo reprovado e deixe claro que os números estão parciais."

RELATORIOS_DIR="$DESTINO" timeout "$TIMEOUT_SEG" "$CLAUDE_BIN" \
    --print --permission-mode acceptEdits \
    --allowedTools "Bash,Read,Write,Edit,Glob,Grep,Skill" \
    "/relatorio-meio-mes data de referência $HOJE. Grave o PDF em $DESTINO.$EXTRA" \
    || { echo "erro: o CLI falhou ou estourou o timeout" >&2; exit 4; }

PDF="$DESTINO/relatorio_meio_mes_$MES.pdf"
[[ -f "$PDF" ]] || { echo "erro: não encontrei $PDF" >&2; exit 4; }
TAM="$(stat -c%s "$PDF")"
(( TAM >= 20000 )) || { echo "erro: $PDF tem só $TAM bytes" >&2; exit 5; }

rm -f "$DADOS"
echo "[meio-mes] pronto: $PDF ($(du -h "$PDF" | cut -f1))"
