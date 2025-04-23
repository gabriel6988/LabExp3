from github import Github, RateLimitExceededException
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# Configurações
HORAS_MINIMAS_REVISAO = 1
ARQUIVO_SAIDA_ANTIGO = "github_pr_reviews_antigo.csv"
ARQUIVO_SAIDA_NOVO = "github_pr_reviews_novo.csv"
DIRETORIO_SAIDA = "D:\\"
MAX_REPOSITORIOS = 200
MIN_PRS = 100
MAX_THREADS = 3
DELAY_REQUISICAO = 15
MAX_TENTATIVAS = 2
ESPERA_LIMITE_TAXA = 3600

class ColetorPRsGitHub:
    def __init__(self):
        self.cliente = self.inicializar_cliente_github()
        self.limite_requisicoes_restante = 5000
        self.ultimo_tempo_requisicao = datetime.now(timezone.utc)
        self.limite_taxa_atingido = False
        self.repositorios_processados = set()
        self.carregar_repositorios_processados()

    def inicializar_cliente_github(self):
        """Inicializa o cliente GitHub com autenticação"""
        TOKEN_GITHUB = os.getenv("GITHUB_TOKEN") or ""
        if not TOKEN_GITHUB:
            raise ValueError("Token do GitHub não encontrado no arquivo .env")
        return Github(
            TOKEN_GITHUB,
            per_page=100,
            timeout=30,
            retry=3,
            pool_size=MAX_THREADS
        )

    def carregar_repositorios_processados(self):
        """Carrega apenas os nomes dos repositórios já processados"""
        caminho_antigo = os.path.join(DIRETORIO_SAIDA, ARQUIVO_SAIDA_ANTIGO)
        if os.path.exists(caminho_antigo):
            try:
                df_antigo = pd.read_csv(caminho_antigo)
                self.repositorios_processados = set(df_antigo['repo'].unique())
                print(f"Encontrados {len(self.repositorios_processados)} repositórios já processados")
            except Exception as e:
                print(f"Erro ao carregar dados antigos: {str(e)}")

    def verificar_limite_taxa(self):
        """Verifica e gerencia os limites de taxa da API"""
        agora = datetime.now(timezone.utc)
        tempo_desde_ultima = (agora - self.ultimo_tempo_requisicao).total_seconds()
        
        if tempo_desde_ultima < DELAY_REQUISICAO:
            time.sleep(DELAY_REQUISICAO - tempo_desde_ultima)
        
        try:
            limite_taxa = self.cliente.get_rate_limit()
            self.limite_requisicoes_restante = limite_taxa.core.remaining
            self.ultimo_tempo_requisicao = datetime.now(timezone.utc)
            
            if self.limite_requisicoes_restante < 100:
                tempo_reset = limite_taxa.core.reset.replace(tzinfo=timezone.utc)
                tempo_espera = (tempo_reset - agora).total_seconds() + 10
                print(f"Limite de taxa próximo. Aguardando {tempo_espera/60:.1f} minutos...")
                time.sleep(tempo_espera)
                return True
        except Exception as e:
            print(f"Não foi possível verificar o limite de taxa: {str(e)}")
        
        return False

    def tratar_limite_taxa_atingido(self):
        """Ação quando o limite de taxa é atingido"""
        print(f"Limite de taxa atingido. Aguardando {ESPERA_LIMITE_TAXA/3600:.1f} horas.")
        time.sleep(ESPERA_LIMITE_TAXA)
        self.limite_taxa_atingido = True
        self.ultimo_tempo_requisicao = datetime.now(timezone.utc)
        return True

    def obter_repositorios_top(self):
        """Obtém os repositórios mais populares, completando até 200 no total"""
        repositorios = []
        tentativas = 0
        repos_needed = MAX_REPOSITORIOS - len(self.repositorios_processados)

        while len(repositorios) < repos_needed and tentativas < MAX_TENTATIVAS:
            try:
                if self.verificar_limite_taxa():
                    continue
                
                busca = self.cliente.search_repositories(
                    "stars:>1",
                    sort="stars",
                    order="desc"
                )

                for repo in busca:
                    if len(repositorios) >= repos_needed:
                        break
                    
                    if repo.full_name in self.repositorios_processados:
                        continue
                    
                    try:
                        if self.verificar_limite_taxa() or self.limite_taxa_atingido:
                            return repositorios
                        
                        contagem_prs = repo.get_pulls(state='all').totalCount

                        if contagem_prs >= MIN_PRS:
                            repositorios.append(repo)
                            print(f"Selecionado {len(repositorios)}/{repos_needed}: {repo.full_name} (PRs: {contagem_prs})")

                    except RateLimitExceededException:
                        self.tratar_limite_taxa_atingido()
                        continue

                    except Exception as e:
                        print(f"Pulando {repo.full_name}: {str(e)}")
                        continue
                break
            
            except RateLimitExceededException:
                self.tratar_limite_taxa_atingido()
                tentativas += 1
            except Exception as e:
                print(f"Erro na busca: {str(e)}")
                tentativas += 1
                time.sleep(60)
    
        return repositorios

    def eh_pr_revisado_por_humano(self, pr):
        """Verifica se o PR foi revisado por humanos"""
        try:
            if pr.state.lower() not in ['closed', 'merged']:
                return False
                
            self.verificar_limite_taxa()
            if pr.get_reviews().totalCount < 1:
                return False
                
            data_criacao = pr.created_at
            data_fechamento = pr.closed_at or pr.merged_at
            if not data_fechamento:
                return False
                
            return (data_fechamento - data_criacao) > timedelta(hours=HORAS_MINIMAS_REVISAO)
        except Exception:
            return False

    def obter_dados_pr_seguro(self, pr, nome_repo, tentativa=1):
        """Obtém dados do PR com tratamento de erros e repetição"""
        try:
            if not self.eh_pr_revisado_por_humano(pr):
                return None

            pr_data = {
                "repo": nome_repo,
                "pr_number": pr.number,
                "state": pr.state.lower(),
                "title_length": len(getattr(pr, 'title', '')),
                "description_length": len(pr.body) if pr.body else 0,
                "description_code_blocks": pr.body.count('```')//2 if pr.body else 0,
                "created_at": getattr(pr, 'created_at', None),
                "closed_at": getattr(pr, 'closed_at', None) or getattr(pr, 'merged_at', None),
                "is_merged": getattr(pr, 'merged', False),
            }

            if pr_data["closed_at"] and pr_data["created_at"]:
                pr_data["review_hours"] = (pr_data["closed_at"] - pr_data["created_at"]).total_seconds() / 3600
            else:
                pr_data["review_hours"] = None

            self.verificar_limite_taxa()
            comments = list(pr.get_comments()) if tentativa == 1 else []
            
            self.verificar_limite_taxa()
            reviews = list(pr.get_reviews()) if tentativa == 1 else []

            pr_data.update({
                "comments": len(comments),
                "review_comments": len(reviews),
                "unique_participants": len(set(
                    [c.user.login for c in comments if c and hasattr(c, 'user') and c.user] +
                    [r.user.login for r in reviews if r and hasattr(r, 'user') and r.user]
                )),
                "additions": getattr(pr, 'additions', 0),
                "deletions": getattr(pr, 'deletions', 0),
                "changed_files": getattr(pr, 'changed_files', 0),
                "changes_size": getattr(pr, 'additions', 0) + getattr(pr, 'deletions', 0),
                "review_count": len(reviews),
                "approval_count": sum(1 for r in reviews if r and getattr(r, 'state', None) == 'APPROVED'),
                "request_changes_count": sum(1 for r in reviews if r and getattr(r, 'state', None) == 'CHANGES_REQUESTED')
            })

            return pr_data

        except RateLimitExceededException:
            if tentativa <= MAX_TENTATIVAS:
                self.tratar_limite_taxa_atingido()
                return self.obter_dados_pr_seguro(pr, nome_repo, tentativa + 1)
            print(f"Máximo de tentativas alcançado para PR: {pr.number}")
            return None
        except Exception as e:
            print(f"Erro ao processar PR #{pr.number}: {str(e)}")
            return None

    def coletar_prs_repositorio(self, repo):
        """Coleta PRs de um único repositório"""
        print(f"\nColetando PRs de {repo.full_name}...")
        dados_pr = []
        try:
            self.verificar_limite_taxa()
            prs = list(repo.get_pulls(state='all', sort='created', direction='desc'))
            
            with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                futuros = {executor.submit(self.obter_dados_pr_seguro, pr, repo.full_name): pr for pr in prs}
                
                for i, futuro in enumerate(as_completed(futuros), 1):
                    try:
                        resultado = futuro.result()
                        if resultado:
                            dados_pr.append(resultado)
                            if i % 20 == 0:
                                print(f"Processados {i}/{len(prs)} PRs ({len(dados_pr)} válidos)")
                    except Exception as e:
                        print(f"Erro ao processar: {str(e)}")
                        
        except RateLimitExceededException:
            self.tratar_limite_taxa_atingido()
            return self.coletar_prs_repositorio(repo)
        except Exception as e:
            print(f"Erro ao buscar PRs de {repo.full_name}: {str(e)}")
        
        print(f"Concluído {repo.full_name} com {len(dados_pr)} PRs válidos")
        return dados_pr

    def salvar_para_csv(self, novos_dados):
        """Salva em novo arquivo, mantendo o histórico intacto"""
        caminho_novo = os.path.join(DIRETORIO_SAIDA, ARQUIVO_SAIDA_NOVO)
        try:
            df_novo = pd.DataFrame(novos_dados)
            
            for col in ['created_at', 'closed_at']:
                if col in df_novo.columns:
                    df_novo[col] = pd.to_datetime(df_novo[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            if os.path.exists(caminho_novo):
                df_existente = pd.read_csv(caminho_novo)
                df_completo = pd.concat([df_existente, df_novo]).drop_duplicates(subset=['repo', 'pr_number'])
            else:
                df_completo = df_novo
                
            df_completo.to_csv(caminho_novo, index=False)
            print(f"Dados salvos em {caminho_novo} (Total: {len(df_completo)} registros)")
            
        except Exception as e:
            print(f"Erro ao salvar CSV: {str(e)}")

    def executar(self):
        """Método principal de execução"""
        try:
            repositorios = self.obter_repositorios_top()
            print(f"\nEncontrados {len(repositorios)} novos repositórios para processar")
            
            novos_dados_pr = []
            for i, repo in enumerate(repositorios, 1):
                print(f"\nProcessando Repositório {i}/{len(repositorios)}: {repo.full_name}")
                dados_repo = self.coletar_prs_repositorio(repo)
                novos_dados_pr.extend(dados_repo)
                self.salvar_para_csv(novos_dados_pr)
                time.sleep(DELAY_REQUISICAO)
            
            print(f"\nAnálise concluída. Coletados {len(novos_dados_pr)} novos PRs")
            
        except Exception as e:
            print(f"Falha no script: {str(e)}")
            raise

if __name__ == "__main__":
    coletor = ColetorPRsGitHub()
    coletor.executar()