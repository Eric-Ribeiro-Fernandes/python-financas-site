from time import sleep
from models.Setores_Acoes import SetoresAcoes

if __name__ == "__main__":

    def atualiza_site() -> None:

        setores_acoes = SetoresAcoes()

        # Coleta de dados de períodos longos
        setores_acoes.generate_reports_5y()
        setores_acoes.generate_dash_2y()

    # Mantém coleta online
    i = 0
    while True:
        atualiza_site()
        i += 1
        print("\nTOTAL DE BUSCAS:", i)
        print("\n\n")
        sleep(60*10)
