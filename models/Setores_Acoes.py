from tempfile import template
import pandas as pd
import numpy as np
import seaborn as sns
import yfinance as web
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen
import json
import os
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import quantstats as qs
from plotly.subplots import make_subplots
from plotly import *


class SetoresAcoes():

    def __init__(self):

        try:
            print("\n\nColetando Tickers...\n")

            self.tickers = self.get_tickers_online()
            print("Tickers atualizados!\n")

            # self.tickers_ = self.instancia_tickers()
            # print("Tickers instanciados!\n")

        except Exception as e:
            print(e)
            print("Falha na coleta dos Tickers online...\n")
            self.tickers = self.get_tickers_local()
            print("Utilizando últimos tickers salvos!\n")
            self.tickers_ = self.instancia_tickers()
            print("Tickers instanciandos!\n")

    @property
    def get_tickers(self) -> dict:
        return self.tickers_

    def get_tickers_local(self) -> dict:
        # '''
        #     Coleta tickers localmente.\n
        #     Os dados estão em formato dict. Onde a chave é o setor e os valores são uma lista de tickers.
        # '''

        with open("./data/util/setor_tickers.json", "r") as file:
            dict_setor_tickers = json.loads(file.read())
            file.close()
        return dict_setor_tickers

    def get_tickers_online(self) -> dict:
        '''
            Coleta tickers no site da B3.\n 
            Escreve os dados no arquivo '.data/util/setor_tickers.json'.\n
            Os dados estão em formato dict. Onde a chave é o setor e os valores são uma lista de tickers.
        '''
        url = "https://www.infomoney.com.br/cotacoes/empresas-b3/"

        tabelas_tickers = pd.read_html(url)

        dict_keys_tickers = ["Bens Industriais", "Consumo Cíclico", "Consumo não Cíclico",
                             "Financeiro", "Materiais Básicos", "Outros", "Petróleo, Gás e Biocombustíveis",
                             "Saúde", "Tecnologia da Informação", "Telecomunicações", "Utilidade Pública"]

        dict_setor_tickers = {}

        for i in range(len(tabelas_tickers)):
            tickers = []
            colunas = tabelas_tickers[i].columns.to_list()

            colunas = colunas[1:]

            for coluna in colunas:

                raw = [ticker + ".SA" for ticker in tabelas_tickers[i]
                       [coluna] if type(ticker) is str]
                tickers.extend(raw)

            dict_setor_tickers[dict_keys_tickers[i].replace(
                " ", "_")] = tickers

        with open("./data/util/setor_tickers.json", "w", encoding='utf8')as file:
            file.write(json.dumps(dict_setor_tickers, indent=1))
            file.close()

        return dict_setor_tickers

    def get_benchmark(self, period) -> pd.DataFrame:
        print("Coletando benchmark...\n")
        benchmark = web.download("^BVSP", period=period)[
            "Adj Close"]
        print("\n")
        benchmark = (benchmark/benchmark.shift(1))-1
        benchmark.index = list(map(lambda x: x.date(), benchmark.index))
        benchmark_acc = np.exp(np.log1p(benchmark).cumsum())
        benchmark = benchmark.rename("Benchmark")
        benchmark_acc = benchmark_acc.rename("Benchmark")

        return benchmark, benchmark_acc

    def instancia_tickers(self) -> np.array(list):
        dict_tickers = {}

        for setor in self.tickers.keys():
            dict_tickers[setor] = [web.Ticker(
                ticker.upper() + ".SA") for ticker in self.tickers[setor]]

        return dict_tickers

    def get_historico_online(self) -> None:
        with open("./data/util/setor_tickers.json", "r") as file:
            tickers = json.loads(file.read())

        for setor in tickers.keys():
            print(f"\nColetando setor de {setor}...\n")
            df = web.download(tickers[setor], period="5y")

            close_ = df["Adj Close"].mean(axis=1)
            open_ = close_.shift(1)
            low_ = df["Low"].mean(axis=1)
            high_ = df["High"].mean(axis=1)
            volume_ = df["Volume"].mean(axis=1)

            df_agregate = pd.DataFrame([open_, low_, high_, close_, volume_]).T
            df_agregate.columns = [
                "open_mean", "low_mean", "hight_mean", "close_mean", "volume_mean"]
            df_agregate.reset_index().to_json(
                f"./data/raw_cotacoes/{setor}.json", orient="records")
        print("Todo histórico de preços dos setores coletados!\n")

    def get_historico_diario_online(self) -> None:
        with open("./data/util/setor_tickers.json", "r") as file:
            tickers = json.loads(file.read())

        for setor in tickers.keys():
            print(f"\nColetando setor de {setor}...\n")
            df = web.download(tickers[setor], period="1d", interval="15m")

            close_ = df["Adj Close"].mean(axis=1)
            open_ = close_.shift(1)
            low_ = df["Low"].mean(axis=1)
            high_ = df["High"].mean(axis=1)
            volume_ = df["Volume"].mean(axis=1)

            df_agregate = pd.DataFrame([open_, low_, high_, close_, volume_]).T
            df_agregate.columns = [
                "open_mean", "low_mean", "hight_mean", "close_mean", "volume_mean"]
            df_agregate.reset_index().to_json(
                f"./data/raw_cotacoes_diario/{setor}.json", orient='records')
        print("\nTodo histórico de preços dos setores coletados!\n".upper())

    def generate_dash_2y(self) -> None:

        print("\nIniciando geração de dashboards dos setores...\n")

        # Coletando benchmark de 2 anos
        benchmark, benchmark_acc = self.get_benchmark("2y")

        # Lendo arquivos coletados
        arquivos = os.listdir("./data/raw_cotacoes/")

        primeira = arquivos.pop()

        df_primeira = (pd.read_json("./data/raw_cotacoes/" + primeira))
        df_primeira = df_primeira[["Date", "close_mean"]]
        df_primeira.rename(
            columns={"close_mean": f"{primeira}".replace(".json", "")}, inplace=True)

        # Tratamento de dados
        for arquivo in arquivos:
            df = (pd.read_json("./data/raw_cotacoes/" + arquivo))
            df = df[["Date", "close_mean"]]
            df.rename(columns={"close_mean": f"{arquivo}".replace(
                ".json", "").replace(" ", "_")}, inplace=True)
            df_primeira = df_primeira.merge(df, on="Date")

            # Criando DataFrames de retornos e retornos acumulados

            dois_anos = datetime.now().replace(year=datetime.now().year-2)
            mask = df_primeira["Date"] >= dois_anos

            df_primeira = df_primeira[mask]

            h = df_primeira.set_index("Date")

            returns = (h/h.shift(1))-1
            returns_acc = np.exp(np.log1p(returns).cumsum())

        # Juntando benchmark nas tabelas de plot
        df_final_acc = pd.merge(returns_acc, benchmark_acc,
                                left_index=True, right_index=True)

        df_final = pd.merge(returns, benchmark,
                            left_index=True, right_index=True)

        df_final_acc = (df_final_acc-1).fillna(method="bfill")
        df_final = (df_final).fillna(method="bfill")

        # Criação do Dashboard
        for setor in returns.columns:

            label_setor = setor.replace("_", " ")
            plot1 = make_subplots(2, 1, shared_xaxes=True, subplot_titles=(
                f"{label_setor} vs Mercado", "Variabilidade dos retornos"), horizontal_spacing=0.2)
            # Linha de Benchmark
            for coluna in df_final_acc.columns:
                # Destaque linha do setor
                if coluna == setor:
                    plot1.add_trace(go.Scatter(x=df_final_acc.index,
                                    y=df_final_acc[coluna][1:-1], name=coluna, showlegend=False, legendgroup="group1", line={"width": 1.5, "color": "blue"}), row=1, col=1)
                # Destaque linha de benchmark
                elif coluna == "Benchmark":
                    plot1.add_trace(go.Scatter(x=df_final_acc.index,
                                    y=df_final_acc[coluna][1:-1], name="Benchmark", showlegend=False, legendgroup="group1", line={"width": 1.5, "color": "gold"}), row=1, col=1)
                else:
                    plot1.add_trace(go.Scatter(x=df_final_acc.index,
                                    y=df_final_acc[coluna][1:-1], name=coluna, showlegend=False, opacity=0.4, legendgroup="group1", line={"width": 0.5}), row=1, col=1)

            for coluna in df_final.columns:
                if coluna == setor:
                    # Destaque linha do setor
                    plot1.add_trace(go.Scatter(x=df_final.index,
                                    y=df_final[coluna][1:-1], name=coluna, showlegend=False, legendgroup="group1", line={"width": 1.5, "color": "blue"}), row=2, col=1)
                elif coluna == "Benchmark":
                    # Destaque linha de benchmark
                    plot1.add_trace(go.Scatter(x=df_final.index,
                                    y=df_final[coluna][1:-1], name="Benchmark", showlegend=False, legendgroup="group1", line={"width": 1.5, "color": "gold"}), row=2, col=1)
                else:
                    plot1.add_trace(go.Scatter(x=df_final.index,
                                    y=df_final[coluna][1:-1], name=coluna, showlegend=False, opacity=0.4, legendgroup="group1", line={"width": 0.5}), row=2, col=1)

            plot1.update_layout(template="plotly_white",
                                yaxis_tickformat='.0%', title="Retornos acumulados nos últimos 2 anos.")

            # Salvando figura em HTML
            my_html = plot1.to_html(include_plotlyjs='cdn')

            with open(f"./templates/dashboards/dash_setor_{setor}.html", "w") as file:
                file.write(my_html)
                print(f"Dashboard do setor {label_setor} gerado!\n")

        print("\nTODOS OS DASHBOARDS GERADOS!\n")

    def generate_reports_5y(self) -> None:
        # Lendo arquivos coletados
        arquivos = os.listdir("./data/raw_cotacoes/")
        primeira = arquivos.pop()
        df_primeira = (pd.read_json("./data/raw_cotacoes/" + primeira))
        df_primeira = df_primeira[["Date", "close_mean"]]
        df_primeira.rename(
            columns={"close_mean": f"{primeira}".replace(".json", "")}, inplace=True)

        for arquivo in arquivos:
            # TRATAMENTO DADOS
            df = (pd.read_json("./data/raw_cotacoes/" + arquivo))

            df = df[["Date", "close_mean"]]
            df.rename(columns={"close_mean": f"{arquivo}".replace(
                ".json", "")}, inplace=True)

            df_primeira = df_primeira.merge(df, on="Date")

            # Criando DataFrames de retornos e retornos acumulados
            h = df_primeira.set_index("Date")
            returns = (h/h.shift(1))-1
            returns_acc = np.exp(np.log1p(returns).cumsum())

        print("Gerando relatórios dos setores...\n")

        # Coletando benchmark de 5 anos
        benchmark, benchmark_acc = self.get_benchmark("5y")

        # Salvando figuras em HTML
        for coluna in returns.columns:
            qs.reports.html(returns[coluna][1:-1], benchmark=benchmark, title=f"Relatório Completo últimos 5 anos.",
                            download_filename=f"./templates/reports/completo_{coluna}.html", output=f"./templates/reports/completo_{coluna}.html")
            print(f"Relatório do setor {coluna} gerado!\n")
        print("TODOS OS RELATÓRIOS FORAM GERADOS!\n")

    def generate_reports_1d(self) -> None:

        # Coleta de benchmark de 1 dia, intervalo de 15m
        benchmark = web.download("^BVSP", period="1d", interval="15m")[
            "Adj Close"]
        benchmark = (benchmark/benchmark.shift(1))-1
        benchmark.index = list(map(lambda x: x.time(), benchmark.index))
        benchmark_acc = np.exp(np.log1p(benchmark).cumsum())
        print(benchmark)

        arquivos = os.listdir("./data/raw_cotacoes_diario/")

        primeira = arquivos.pop()

        df_primeira = (pd.read_json("./data/raw_cotacoes_diario/" + primeira))
        df_primeira = df_primeira[["index", "close_mean"]]
        df_primeira.rename(
            columns={"close_mean": f"{primeira}".replace(".json", "")}, inplace=True)

        df_primeira["index"] = (
            df_primeira["index"] / 1000).map(lambda x: datetime.fromtimestamp(x).time())
        for arquivo in arquivos:
            # TRATAMENTO DADOS
            df = (pd.read_json("./data/raw_cotacoes_diario/" + arquivo))

            df = df[["index", "close_mean"]]
            df.rename(columns={"close_mean": f"{arquivo}".replace(
                ".json", "")}, inplace=True)
            df["index"] = (
                df["index"] / 1000).map(lambda x: datetime.fromtimestamp(x).time())

            df_primeira = df_primeira.merge(df, on="index")

            h = df_primeira.set_index("index")
            returns = (h/h.shift(1))-1

            returns_acc = np.exp(np.log1p(returns).cumsum())

        print("Gerando relatórios dos setores...\n")

        for coluna in returns.columns:
           # print(pd.merge(benchmark,returns[coluna], left_on = benchmark.index, right_on = returns.index))
            df_merge = pd.merge(
                benchmark_acc, returns_acc[coluna], left_on=benchmark.index, right_on=returns_acc.index)
            print(df_merge)
            fig = px.line(df_merge.set_index("key_0"))
            fig.show()
            print(f"Relatório do setor {coluna} gerado!\n")
        print("TODOS OS RELATÓRIOS FORAM GERADOS!\n")

    def plot_heatmap_1d(self) -> None:
        arquivos = os.listdir("./data/raw_cotacoes/")

        primeira = arquivos.pop()

        df_primeira = (pd.read_json("./data/raw_cotacoes_diario/" + primeira))
        df_primeira = df_primeira[["index", "close_mean"]]
        df_primeira.rename(
            columns={"close_mean": f"{primeira}".replace(".json", "")}, inplace=True)

        df_primeira["index"] = (
            df_primeira["index"] / 1000).map(lambda x: datetime.fromtimestamp(x))

        # df_primeira.drop(columns= "index", inplace = True)

        for arquivo in arquivos:
            # TRATAMENTO DADOS
            df = (pd.read_json("./data/raw_cotacoes_diario/" + arquivo))

            df = df[["index", "close_mean"]]
            df.rename(columns={"close_mean": f"{arquivo}".replace(
                ".json", "")}, inplace=True)

            df["index"] = (
                df["index"] / 1000).map(lambda x: datetime.fromtimestamp(x))

            # df.drop(columns= "index", inplace = True)

            df_primeira = df_primeira.merge(df, on="index")

            h = df_primeira.set_index("index")
            returns = (h/h.shift(1))-1

            returns_acc = np.exp(np.log1p(returns).cumsum())

        print("Gerando relatórios dos setores...\n")

        #     # fig = sns.heatmap(returns[coluna],  title=f"Relatório Completo {coluna}")

        data = [
            go.Heatmap(
                z=returns.T,
                x=returns.index,
                y=returns.columns,
                # colorscale="rdylgn",
                autocolorscale=False,

            )]

        layout = go.Layout(
            xaxis_nticks=12,
            title="Retornos setores hoje"
        )

        fig = go.Figure(data=data, layout=layout)

        # fig.data[0].update(zmin=-1.5, zmax=1.5)
        # fig = px.imshow(data, color_continuous_scale='RdBu_r')
        # fig.show()

        # print(f"Relatório do setor {coluna} gerado!\n")
        print("TODOS OS RELATÓRIOS FORAM GERADOS!\n")
        return data[0]
