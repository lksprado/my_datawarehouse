"""Camada compartilhada dos relatórios do domínio finanças.

Dois relatórios consomem este pacote, com cadências diferentes:

    relatorio-financas    fechamento do mês anterior, rodado nos primeiros
                          dias do mês seguinte — quatro PDFs
    relatorio-meio-mes    acompanhamento do mês em andamento, rodado entre os
                          dias 15 e 20 — um PDF

O que mora aqui é o que os dois precisam ver igual: os parâmetros da política
(`politica`), a formatação pt-BR (`formato`), os agregados (`calculo`), os
gráficos SVG (`graficos`) e os blocos de HTML (`blocos`). O que é específico de
um relatório — capa, rodapé, tarja de prontidão, ordem das seções — fica no
montador daquele relatório.

Motivo de o pacote existir: os parâmetros da política já estiveram duplicados
entre `_docs_financas.md` e o montador, e divergiram em silêncio. Um segundo
montador com uma terceira cópia tornaria isso rotina. Agora há uma cópia em
Python — `politica.py` — e a regra do CLAUDE.md continua valendo: mudou no
Markdown, muda aqui na mesma passada.
"""
