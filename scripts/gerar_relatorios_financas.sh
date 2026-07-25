#!/usr/bin/env bash
# Entrypoint headless dos relatórios financeiros mensais — para o Airflow.
#
# Roda a skill `/relatorio-financas` em modo não interativo e devolve os PDFs
# em RELATORIOS_DIR. Deve ser chamado DEPOIS do `dbt run` do seletor financas,
# na mesma DAG.
#
#   gerar_relatorios_financas.sh [AAAA-MM]
#
# Sem argumento: mês anterior ao atual.
#
# Variáveis de ambiente:
#   RELATORIOS_DIR   destino dos PDFs (default: <projeto>/relatorios/AAAA-MM)
#   DBT_PROFILES_DIR diretório do profiles.yml (default: ~/.dbt)
#   CLAUDE_BIN       caminho do CLI (default: `claude` no PATH)
#   TIMEOUT_SEG      teto de execução (default: 900)
#
# Saída: 0 com os dois PDFs gerados; != 0 caso contrário.
#
# Exemplo de uso na DAG (Astro Cosmos):
#
#   BashOperator(
#       task_id="relatorios_financas",
#       bash_command=(
#           "{{ var.value.dbt_project_dir }}/scripts/"
#           "gerar_relatorios_financas.sh {{ data_interval_start | ds_format('%Y-%m-%d', '%Y-%m') }}"
#       ),
#       env={"RELATORIOS_DIR": "/opt/airflow/relatorios",
#            "ANTHROPIC_API_KEY": "{{ var.value.anthropic_api_key }}"},
#       append_env=True,
#   )
set -euo pipefail

PROJETO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MES_REF="${1:-$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m)}"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
TIMEOUT_SEG="${TIMEOUT_SEG:-900}"

if ! [[ "$MES_REF" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "erro: mês inválido '$MES_REF' (esperado AAAA-MM)" >&2
    exit 2
fi

DESTINO="${RELATORIOS_DIR:-$PROJETO/relatorios/$MES_REF}"
mkdir -p "$DESTINO"

command -v "$CLAUDE_BIN" >/dev/null 2>&1 || {
    echo "erro: CLI '$CLAUDE_BIN' não encontrado no PATH" >&2; exit 3; }

echo "[relatorios] projeto=$PROJETO mes=$MES_REF destino=$DESTINO"

# Falha cedo se o warehouse não estiver acessível — erro claro em vez de um
# agente gastando tokens para descobrir que o banco caiu.
"$PROJETO/.claude/skills/relatorio-financas/scripts/extrair_dados.sh" \
    "$MES_REF" "$DESTINO/.dados_$MES_REF.json"

cd "$PROJETO"
set +e
RELATORIOS_DIR="$DESTINO" timeout "$TIMEOUT_SEG" "$CLAUDE_BIN" \
    --print \
    --permission-mode acceptEdits \
    --allowedTools "Bash,Read,Write,Edit,Glob,Grep,Skill" \
    "/relatorio-financas mês de referência $MES_REF-01. Grave os PDFs em $DESTINO. Modo não interativo: não faça perguntas, use os defaults do glossário para qualquer parâmetro marcado [CONFIRMAR] e registre no rodapé quais premissas foram usadas."
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

rm -f "$DESTINO/.dados_$MES_REF.json"
echo "[relatorios] concluído para $MES_REF"
