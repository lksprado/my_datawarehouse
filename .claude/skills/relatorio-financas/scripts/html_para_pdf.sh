#!/usr/bin/env bash
# Converte um relatório HTML em PDF A4 via Chrome headless.
#
#   html_para_pdf.sh ENTRADA.html [SAIDA.pdf]
#
# O HTML precisa ser autocontido (CSS embutido, SVG inline, sem recurso remoto):
# o Chrome roda sem rede e qualquer referência externa sai em branco no PDF.
set -euo pipefail

ENTRADA="${1:?uso: html_para_pdf.sh ENTRADA.html [SAIDA.pdf]}"
SAIDA="${2:-${ENTRADA%.html}.pdf}"

[[ -f "$ENTRADA" ]] || { echo "erro: não encontrei $ENTRADA" >&2; exit 2; }

NAVEGADOR=""
for c in google-chrome chromium chromium-browser google-chrome-stable; do
    command -v "$c" >/dev/null 2>&1 && { NAVEGADOR="$c"; break; }
done
[[ -n "$NAVEGADOR" ]] || { echo "erro: Chrome/Chromium não encontrado no PATH" >&2; exit 3; }

ENTRADA_ABS="$(readlink -f "$ENTRADA")"
mkdir -p "$(dirname "$SAIDA")"
PERFIL="$(mktemp -d)"
trap 'rm -rf "$PERFIL"' EXIT

# --virtual-time-budget dá tempo de layout/fontes antes da captura.
timeout 120 "$NAVEGADOR" \
    --headless \
    --disable-gpu \
    --no-sandbox \
    --no-first-run \
    --disable-extensions \
    --user-data-dir="$PERFIL" \
    --virtual-time-budget=8000 \
    --no-pdf-header-footer \
    --print-to-pdf="$SAIDA" \
    "file://$ENTRADA_ABS" 2>/dev/null

[[ -s "$SAIDA" ]] || { echo "erro: PDF não foi gerado ou saiu vazio" >&2; exit 4; }
echo "PDF gerado: $SAIDA ($(du -h "$SAIDA" | cut -f1))"
