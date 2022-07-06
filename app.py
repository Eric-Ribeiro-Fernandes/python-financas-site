from flask import Flask, render_template, send_from_directory
from time import sleep

import main

app = Flask(__name__)

# Inicializando rotinas de ETL
# Definindo rotas de templates acessórios státicos


@app.route("/<path:path>")
def static_dir(path):
    return send_from_directory(".", path)


# Definindo rotas do site
@app.route("/")
def carrega_pagina():

    lista = ["Bens_Industriais", "Consumo_Cíclico", "Financeiro",
             "Materiais_Básicos", "Outros", "Petróleo,_Gás_e_Biocombustíveis",
             "Saúde", "Tecnologia_da_Informação", "Telecomunicações", "Utilidade_Pública"]

    return render_template("/screens/setores.html", setores=lista)


app.run(debug=True)
