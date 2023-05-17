import requests
from datetime import datetime, timedelta
from django.shortcuts import render

from github_pulls.settings import env


def get_pulls(request):
    if request.method == 'POST':
        # Get username, repository name and number of days from form
        username = request.POST['username']
        repository = request.POST['repository']
        days = int(request.POST['days'])
        data_type = request.POST['data_type']

        # Set up authentication with access token
        access_token = env('TOKEN')
        headers = {'Authorization': f'token {access_token}'}

        # Set up API endpoint
        api_endpoint = f'https://api.github.com/repos/{username}/{repository}/{data_type}'

        # Calculate the start date based on the specified number of days
        start_date = datetime.now() - timedelta(days=days)

        # Retrieve list of pull requests or issues
        authors_count = dict()

        for page_number in range(1, 8):
            response = requests.get(api_endpoint + f"?page={page_number}", headers=headers, params={'state': 'open'})
            data = response.json()
            if not data:
                break

            # Extract list of authors within the specified period
            authors = [item['user']['login'] for item in data if datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%SZ') >= start_date]

            for author in authors:
                if author not in authors_count:
                    authors_count[author] = 0
                authors_count[author] += 1

        context = {
            'username': username,
            'repository': repository,
            'authors_count': authors_count,
            'total_authors': len(authors_count),
            'total_count': sum(authors_count.values()),
            'data_type': data_type
        }

        return render(request, 'pulls.html', context)

    return render(request, 'index.html')
