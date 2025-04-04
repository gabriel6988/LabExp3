import os
import time
from datetime import datetime, timedelta
from github import Github, RateLimitExceededException
import pandas as pd
from dotenv import load_dotenv

# Carregar variáveis ​​de ambiente do arquivo .env
load_dotenv()

# Constantes
MIN_REVIEW_HOURS = 1  # Duração mínima da revisão a ser considerada (1 hora)
OUTPUT_FILENAME = "github_pr_reviews.csv" # Nome do arquivo de saída

def initialize_github_client():
    """Inicializar e retornar o cliente GitHub autenticado."""
    GITHUB_TOKEN = "" # Colocar a sua Token aqui
    return Github(GITHUB_TOKEN, per_page=100)  # per_page = 100, reduz chamadas de API

def handle_rate_limit(github_client):
    """Lida com os limites de taxa da API do GitHub ao aguardar até que o limite seja redefinido."""
    rate_limit = github_client.get_rate_limit()
    reset_time = rate_limit.core.reset
    wait_seconds = (reset_time - datetime.utcnow()).total_seconds() + 10  # Buffer
    print(f"Limite de taxa excedido. Aguardando {wait_seconds/60:.1f} minutos...")
    time.sleep(max(wait_seconds, 0))
    return True

def get_top_repositories(github_client, limit=200, min_prs=100):
    """
    Identifique repositórios populares do GitHub com muitos PRs.
    
    Args:
        github_client: Cliente GitHub autenticado
        limit: Número máximo de top repositórios a serem considerados
        min_prs: Número mínimo de PRs que um repositório deve ter para ser incluído
    
    Returns:
        Lista de objetos do repositório que atendem aos critérios
    """
    try:
        print(f"Pesquisando os top {limit} repositórios com pelo menos {min_prs} PRs...")
        
        # Pesquisar repositórios populares classificados por estrelas
        query = "stars:>1 sort:stars-desc"
        repos = github_client.search_repositories(query)[:limit]
        
        filtered_repos = []
        seen_repos = set()  # Rastreie repositórios vistos para evitar duplicatas
        
        for repo in repos:
            try:
                if repo.full_name in seen_repos:
                    continue
                
                # Verifique a contagem de RP
                prs = repo.get_pulls(state='all', sort='created', direction='desc')
                if prs.totalCount >= min_prs:
                    filtered_repos.append(repo)
                    seen_repos.add(repo.full_name)
                    print(f"Selected: {repo.full_name} (PRs: {prs.totalCount})")
                    
                    # Saída antecipada se tivermos repositórios suficientes
                    if len(filtered_repos) >= limit:
                        break
                        
            except RateLimitExceededException:
                handle_rate_limit(github_client)
                continue
            except Exception as e:
                print(f"Error checking {repo.full_name}: {str(e)}")
                continue
        
        return filtered_repos
        
    except RateLimitExceededException:
        handle_rate_limit(github_client)
        return get_top_repositories(github_client, limit, min_prs)

def is_human_reviewed_pr(pr, min_hours=MIN_REVIEW_HOURS):
    """
    Determinar se um PR foi revisado por humanos com base na duração da revisão.
    
    Args:
        pr: PullRequest object
        min_hours: Duração mínima da revisão para ser considerada uma revisão humana
    
    Returns:
        bool: Verdadeiro se o PR atender aos critérios de revisão humana
    """
    if pr.state.lower() not in ['closed', 'merged']:
        return False
    
    # Deve ter pelo menos uma revisão
    if pr.get_reviews().totalCount < 1:
        return False
    
    created_at = pr.created_at
    closed_at = pr.closed_at or pr.merged_at
    
    # Deve ter uma data de closed/merged
    if not closed_at:
        return False
    
    # A revisão deve ter levado pelo menos uma hora
    review_duration = closed_at - created_at
    return review_duration > timedelta(hours=min_hours)

def fetch_pr_data(github_client, repo):
    """
    Colete dados de RP de um repositório que atenda aos critérios de revisão humana.
    
    Args:
        github_client: Cliente GitHub autenticado
        repo: Objeto de repositório para analisar
    
    Returns:
        Lista contendo dados de PRs
    """
    pr_data = []
    print(f"\nColetando PRs de {repo.full_name}...")
    
    try:
        # Obtenha todos os PRs (merged ou closed) classificados por data de criação
        prs = repo.get_pulls(state='all', sort='created', direction='desc')
        
        for pr in prs:
            try:
                # Pular se não for revisado por humanos
                if not is_human_reviewed_pr(pr):
                    continue
                
                # Métricas Coletadas
                pr_info = {
                    "repo": repo.full_name,
                    "pr_number": pr.number,
                    "state": pr.state.lower(),
                    "title": pr.title[:200],
                    "author": pr.user.login if pr.user else None,
                    "author_type": pr.user.type if pr.user else None,
                    "created_at": pr.created_at,
                    "closed_at": pr.closed_at or pr.merged_at,
                    "review_hours": (pr.closed_at - pr.created_at).total_seconds() / 3600,
                    "comments": pr.comments,
                    "review_comments": pr.review_comments,
                    "additions": pr.additions,
                    "deletions": pr.deletions,
                    "changed_files": pr.changed_files,
                    "review_count": pr.get_reviews().totalCount,
                    "is_merged": pr.is_merged()
                }
                
                pr_data.append(pr_info)
                
                # Indicador de progresso
                if len(pr_data) % 20 == 0:
                    print(f"{len(pr_data)} PRs coletadas...")
                    
            except RateLimitExceededException:
                handle_rate_limit(github_client)
                continue
            except Exception as e:
                print(f"Erro ao processar PR #{pr.number}: {str(e)}")
                continue
                
    except RateLimitExceededException:
        handle_rate_limit(github_client)
        return fetch_pr_data(github_client, repo)
    except Exception as e:
        print(f"Erro ao buscar PRs para {repo.full_name}: {str(e)}")
    
    print(f"Concluída a coleta de {len(pr_data)} PRs de {repo.full_name}")
    return pr_data

def save_to_csv(data, filename):
    """Salvar os dados coletados em CSV."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(script_dir, filename)
    
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

def main():
    try:
        # Inicializar cliente GitHub
        g = initialize_github_client()
        
        # Etapa 1: Obtenha os top repositórios
        repositories = get_top_repositories(g, limit=200, min_prs=100)
        
        # Etapa 2: Coletar dados PR de cada repositório
        all_pr_data = []
        for repo in repositories:
            pr_data = fetch_pr_data(g, repo)
            all_pr_data.extend(pr_data)
            
            # Salvar o progresso após cada repositório
            save_to_csv(all_pr_data, OUTPUT_FILENAME)
            
            time.sleep(10)
        
        # Salvamento final
        save_to_csv(all_pr_data, OUTPUT_FILENAME)
        print(f"\nAnálise completa. {len(all_pr_data)} PRs coletadas.")
        
    except Exception as e:
        print(f"O script falhou: {str(e)}")

if __name__ == "__main__":
    main()