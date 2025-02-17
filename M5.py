import aiohttp
import asyncio
import csv
import random
from bs4 import BeautifulSoup

# Cabeçalhos para evitar bloqueio do site
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
}

# URL principal do IMDB
IMDB_URL = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'

# Número máximo de conexões simultâneas
MAX_CONCURRENT_REQUESTS = 10

# Semáforo para limitar requisições simultâneas
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def fetch(session, url):
    """Faz uma requisição assíncrona para uma URL"""
    async with semaphore:
        await asyncio.sleep(random.uniform(0, 0.2))  # Pequeno atraso aleatório para evitar bloqueio
        async with session.get(url, headers=HEADERS) as response:
            return await response.text()

async def extract_movie_details(session, movie_link):
    """Extrai detalhes de um filme específico"""
    html = await fetch(session, movie_link)
    soup = BeautifulSoup(html, 'html.parser')

    title = date = rating = plot_text = None

    # Título do filme
    title_tag = soup.find('h1')
    if title_tag:
        title = title_tag.text.strip()

    # Data de lançamento
    date_tag = soup.find('a', href=lambda href: href and 'releaseinfo' in href)
    if date_tag:
        date = date_tag.text.strip()

    # Classificação do filme
    rating_tag = soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
    if rating_tag:
        rating = rating_tag.text.strip()

    # Sinopse do filme
    plot_tag = soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
    if plot_tag:
        plot_text = plot_tag.text.strip()

    # Salvar no CSV
    if all([title, date, rating, plot_text]):
        print(f'✔ {title} | {date} | {rating} | {plot_text[:50]}...')
        with open('movies_async.csv', mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([title, date, rating, plot_text])

async def extract_movies():
    """Extrai a lista de filmes mais populares do IMDB"""
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, IMDB_URL)
        soup = BeautifulSoup(html, 'html.parser')

        # Encontrando os links dos filmes
        movies_list = soup.find('div', {'data-testid': 'chart-layout-main-column'})
        if not movies_list:
            print("❌ Falha ao encontrar a lista de filmes")
            return
        
        movie_links = [
            'https://imdb.com' + movie.find('a')['href']
            for movie in movies_list.find_all('li') if movie.find('a')
        ]

        # Executa as chamadas assíncronas para os detalhes dos filmes
        tasks = [extract_movie_details(session, link) for link in movie_links]
        await asyncio.gather(*tasks)

def main():
    """Executa o scraping assíncrono"""
    asyncio.run(extract_movies())

if __name__ == '__main__':
    main()
