from github import Github, RateLimitExceededException
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

MIN_REVIEW_HOURS = 1 # Duração mínima da revisão a ser considerada (1 hora)
OUTPUT_FILENAME = "github_pr_reviews.csv" # Nome do arquivo onde os dados são salvos
OUTPUT_DIR = "D:\\" # Local onde os arquivos são salvos
MAX_REPOSITORIES = 200 # Máximo de Repositórios a serem considerados.
MIN_PRS = 100 # Mínimo de PRs que os repositórios precisam ter.
MAX_WORKERS = 3
API_DELAY = 15
MAX_RETRIES = 2
RATE_LIMIT_WAIT = 3600

class GitHubPRCollector:
    def __init__(self):
        self.client = self.initialize_github_client()
        self.rate_limit_remaining = 5000
        self.last_request_time = datetime.now(timezone.utc)
        self.hard_rate_limit_hit = False

    def initialize_github_client(self):
        """Inicialize o cliente GitHub com autenticação adequada."""
        GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or "" # Token do Github
        if not GITHUB_TOKEN:
            raise ValueError("Token do GitHub não encontrado.")
        return Github(
            GITHUB_TOKEN,
            per_page=100,
            timeout=30,
            retry=3,
            pool_size=MAX_WORKERS
        )

    def check_rate_limit(self):
        """Verifique e manipule limites de taxa com tratamento adequado de fuso horário."""
        now = datetime.now(timezone.utc)
        time_since_last = (now - self.last_request_time).total_seconds()
        
        if time_since_last < API_DELAY:
            time.sleep(API_DELAY - time_since_last)
        
        try:
            rate_limit = self.client.get_rate_limit()
            self.rate_limit_remaining = rate_limit.core.remaining
            self.last_request_time = datetime.now(timezone.utc)
            
            if self.rate_limit_remaining < 100:
                reset_time = rate_limit.core.reset.replace(tzinfo=timezone.utc)
                wait_time = (reset_time - now).total_seconds() + 10
                print(f"Aproximando-se do limite de taxa. Aguardando {wait_time/60:.1f} minutos...")
                time.sleep(wait_time)
                return True
        except Exception as e:
            print(f"Não foi possível verificar o limite de taxa: {str(e)}.")
        
        return False

    def handle_rate_limit_exceeded(self):
        """Lidar com quando realmente atingimos o limite de taxa."""
        print(f"Limite de taxa atingido. Parando por {RATE_LIMIT_WAIT/3600:.1f} horas.")
        time.sleep(RATE_LIMIT_WAIT)
        self.hard_rate_limit_hit = True
        self.last_request_time = datetime.now(timezone.utc)
        return True

    def get_top_repositories(self):
        """Obtenha repositórios com tratamento de erros robusto."""
        repos = []
        attempts = 0
        
        while len(repos) < MAX_REPOSITORIES and attempts < MAX_RETRIES:
            try:
                if self.check_rate_limit():
                    continue
                    
                search = self.client.search_repositories(
                    "stars:>1",
                    sort="stars",
                    order="desc"
                )
                
                for repo in search:
                    if len(repos) >= MAX_REPOSITORIES:
                        break
                        
                    try:
                        if self.check_rate_limit() or self.hard_rate_limit_hit:
                            return repos
                            
                        pr_count = repo.get_pulls(state='all').totalCount
                        if pr_count >= MIN_PRS:
                            repos.append(repo)
                            print(f"Selecionado {len(repos)}/{MAX_REPOSITORIES}: {repo.full_name} (PRs: {pr_count}).")
                    except RateLimitExceededException:
                        self.handle_rate_limit_exceeded()
                        continue
                    except Exception as e:
                        print(f"Pulando {repo.full_name}: {str(e)}.")
                        continue
                        
                break
                
            except RateLimitExceededException:
                self.handle_rate_limit_exceeded()
                attempts += 1
            except Exception as e:
                print(f"Erro de pesquisa: {str(e)}.")
                attempts += 1
                time.sleep(60)
                
        return repos[:MAX_REPOSITORIES]

    def is_human_reviewed_pr(self, pr):
        """Verifique o status da revisão do PR com tratamento de erros."""
        try:
            if pr.state.lower() not in ['closed', 'merged']:
                return False
                
            self.check_rate_limit()
            if pr.get_reviews().totalCount < 1:
                return False
                
            created_at = pr.created_at
            closed_at = pr.closed_at or pr.merged_at
            if not closed_at:
                return False
                
            return (closed_at - created_at) > timedelta(hours=MIN_REVIEW_HOURS)
        except Exception:
            return False

    def safe_get_pr_data(self, pr, repo_name, attempt=1):
        """Obtenha dados do PR com segurança e com lógica de repetição."""
        try:
            if not self.is_human_reviewed_pr(pr):
                return None

            pr_data = {
                "repo": repo_name,
                "pr_number": pr.number,
                "state": pr.state.lower(),
                "title_length": len(getattr(pr, 'title', '')),
                "description_length": len(pr.body) if pr.body else 0, # RQ03, RQ07
                "description_code_blocks": pr.body.count('```')//2 if pr.body else 0,
                "author": getattr(pr.user, 'login', None) if pr.user else None,
                "author_type": getattr(pr.user, 'type', None) if pr.user else None,
                "created_at": getattr(pr, 'created_at', None),
                "closed_at": getattr(pr, 'closed_at', None) or getattr(pr, 'merged_at', None),
                "is_merged": getattr(pr, 'merged', False), # RQ01, RQ02, RQ03, RQ04
                "labels": ",".join(label.name for label in pr.labels) if hasattr(pr, 'labels') else None,
            }

            if pr_data["closed_at"] and pr_data["created_at"]:
                pr_data["review_hours"] = (pr_data["closed_at"] - pr_data["created_at"]).total_seconds() / 3600 # RQ02, RQ06
            else:
                pr_data["review_hours"] = None

            self.check_rate_limit()
            comments = list(pr.get_comments()) if attempt == 1 else []
            
            self.check_rate_limit()
            reviews = list(pr.get_reviews()) if attempt == 1 else []

            pr_data.update({
                "comments": len(comments), # RQ04, RQ08
                "review_comments": len(reviews), # RQ04, RQ08
                "unique_participants": len(set(
                    [c.user.login for c in comments if c and hasattr(c, 'user') and c.user] +
                    [r.user.login for r in reviews if r and hasattr(r, 'user') and r.user]
                )), # RQ04, RQ08
                "additions": getattr(pr, 'additions', 0),
                "deletions": getattr(pr, 'deletions', 0),
                "changed_files": getattr(pr, 'changed_files', 0), # RQ01, RQ05
                "changes_size": getattr(pr, 'additions', 0) + getattr(pr, 'deletions', 0), # RQ01, RQ05
                "review_count": len(reviews), # RQ05, RQ06, RQ07, RQ08
                "approval_count": sum(1 for r in reviews if r and getattr(r, 'state', None) == 'APPROVED'),
                "request_changes_count": sum(1 for r in reviews if r and getattr(r, 'state', None) == 'CHANGES_REQUESTED')
            })

            return pr_data

        except RateLimitExceededException:
            if attempt <= MAX_RETRIES:
                self.handle_rate_limit_exceeded()
                return self.safe_get_pr_data(pr, repo_name, attempt + 1)
            print(f"Máximo de tentativas alcançadas para PR: {pr.number}.")
            return None
        except Exception as e:
            print(f"Erro ao processar PR #{pr.number}: {str(e)}.")
            return None

    def fetch_repository_prs(self, repo):
        """Buscar PRs para um único repositório."""
        print(f"\nColetando PRs de {repo.full_name}...")
        pr_data = []
        try:
            self.check_rate_limit()
            prs = list(repo.get_pulls(state='all', sort='created', direction='desc'))
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.safe_get_pr_data, pr, repo.full_name): pr for pr in prs}
                
                for i, future in enumerate(as_completed(futures), 1):
                    try:
                        result = future.result()
                        if result:
                            pr_data.append(result)
                            if i % 20 == 0: # Indicador de progresso
                                print(f"Processado {i}/{len(prs)} PRs ({len(pr_data)} valídas).")
                    except Exception as e:
                        print(f"Erro ao processar: {str(e)}.")
                        
        except RateLimitExceededException:
            self.handle_rate_limit_exceeded()
            return self.fetch_repository_prs(repo)
        except Exception as e:
            print(f"Erro ao buscar PRs de {repo.full_name}: {str(e)}.")
        
        print(f"Concluído {repo.full_name} com {len(pr_data)} PRs válidas.")
        return pr_data

    def save_to_csv(self, data):
        """Salvar dados com formatação adequada."""
        filepath = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
        try:
            df = pd.DataFrame(data)
            for col in ['created_at', 'closed_at']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
            df.to_csv(filepath, index=False)
            print(f"Dados salvos em {filepath}.")
        except Exception as e:
            print(f"Erro ao salvar CSV: {str(e)}.")

    def run(self):
        """Método de execução principal."""
        try:
            repos = self.get_top_repositories()
            print(f"\n Foram encontrados {len(repos)} repositórios para processar.")
            
            all_pr_data = []
            for i, repo in enumerate(repos, 1):
                print(f"\nProcessando Repositório {i}/{len(repos)}: {repo.full_name}.")
                repo_data = self.fetch_repository_prs(repo)
                all_pr_data.extend(repo_data)
                self.save_to_csv(all_pr_data)
                time.sleep(API_DELAY)
            
            self.save_to_csv(all_pr_data)
            print(f"\nAnálise completa. Coletada {len(all_pr_data)} PRs de {len(repos)} repositórios.")
            
        except Exception as e:
            print(f"O script falhou: {str(e)}.")
            raise

if __name__ == "__main__":
    collector = GitHubPRCollector()
    collector.run()